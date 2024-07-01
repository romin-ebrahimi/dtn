from iqfeed import config
import logging
import pandas as pd
import socket

log = logging.getLogger(__name__)


class Connection:
    def __init__(
        self,
        port: int,
        host: str = "127.0.0.1",
        name: str = "",
        timeout: int = 300,
    ):
        self._port = port
        self._host = host
        self._name = name
        self._connected = False
        self._symbol = None
        self._fundamental_fieldnames = []
        self._update_fieldnames = []
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.settimeout(timeout)

    def connect(self) -> None:
        """
        Connect to the given host:port combination. Then initialize the
        connection by setting the protocol, reading any startup messages, and
        setting the self._connected status.
        """
        # The admin port and streaming ports for L1 / L2 return messages upon
        # connection. However, the lookup port returns nothing.
        try:
            self.connection.connect((self._host, self._port))
            if self._port in [9100, 9300]:
                # Nothing to read when initially connecting to 9100 or 9300.
                self._connected = True
                self.set_protocol()
            elif self._port in [5009, 9200]:
                # There are 5 response messages when connecting to L1 / L2.
                messages = self.read()
                if messages is not None:
                    self._connected = self.check_startup(messages)
                    self.set_protocol()
                if self._connected:
                    self._fundamental_fieldnames = self.request_fieldnames("F")
                    self._update_fieldnames = self.request_fieldnames("Q")
            else:
                self._connected = False
        except ConnectionRefusedError:
            log.error(f"Connection refused {self._host}:{self._port}")

        # Log whether the connection was successful or not.
        if self._connected:
            log.info(f"Connected {self._host}:{self._port}")
        else:
            log.warning(f"Connection fail {self._host}:{self._port}")

        return None

    def disconnect(self) -> None:
        """
        Disconnect from the API and change status of the connection.
        """
        # self.connection.shutdown(socket.SHUT_RDWR) # Err: Bad file descriptor.
        self.connection.close()
        self._connected = False

        return None

    def write(self, message: str) -> None:
        """
        Write a message to the socket TCP connection.

        Args:
            message: (str)
        Returns:
            None
        """
        self.connection.send(message.encode("utf-8"))

        return None

    def read(self) -> str | pd.DataFrame:
        """
        Read messages from the stream connection. Separate message types have
        different initial encodings, which indicate the message type.

        E is an error message.
        F is a fundamental message.
        N is a news headline message.
        P is a summary message.
        Q is an update message.
        R is a regional update message if regional price updates are enabled.
        S is a system message. e.g. connected / disconnected.
        T is a timestamp message.
        """
        # Data received as a string. Continue receiving chunks until the new
        # line character is received, which denotes the end of a message.
        msg = self.connection.recv(1024).decode("utf-8")
        while msg is not None and msg != "" and msg[-1] != "\n":
            msg += self.connection.recv(1024).decode("utf-8")

        messages = msg.split("\n")
        # When only one message exists, splitting creates an empty list element.
        if msg is not None and messages[-1] == "":
            del messages[-1]

        # Check if there are errors returned by the API and add to logging.
        messages = self.check_error(messages=messages)
        # Check if message is a system message, which starts with "S".
        is_system_message = self.check_system_messages(messages=messages)

        if self._port == 9300:
            data = self.process_admin(messages=messages)
        elif self._port in [5009, 9200] and not is_system_message:
            data = self.process_stream(messages=messages)
        else:
            data = messages

        return data

    def set_protocol(self) -> None:
        """
        The protocol must be set for every connection even if there are multiple
        connections to the same port.
        """
        msg_protocol = f"S,SET PROTOCOL,{config.PROTOCOL}\r\n"
        self.write(message=msg_protocol)
        self.read()

        return None

    def symbol_watch(self, symbol: str) -> None:
        """
        Begins watching a symbol for Level 1 or Level 2 updates.

        Args:
            symbol: (str) of the symbol to watch.
        Returns:
            None
        """
        self._symbol = symbol
        if self._port == 5009:
            # L1 port begin watching for symbol updates.
            msg = f"w{self._symbol}\r\n"
        elif self._port == 9200:
            # L2 port begin watching symbol for Market By Order (MBO) updates.
            msg = f"WOR,{self._symbol}\r\r\n"
        else:
            log.error("Must be L1 or L2 connection to stream symbol.")

        self.write(msg)
        # Update the expected field names coming from the API.
        self.request_fieldnames(field_type="Q")

        return None

    def symbol_watch_interval(
        self,
        symbol: str,
        interval: int = 1,
    ) -> None:
        """
        Begins streaming interval bars for a given symbol. Streams starts with
        7 days of historical data to fill in any gaps from the prod server.

        Args:
            symbol: (str) of the symbol to watch.
            interval: (int) seconds of the interval [1, 300] or [300, 3600]
                and divisible evenly by 60. Default is 1 second bars.

        Returns:
            None
        """
        self._symbol = symbol
        msg = f"BW,{self._symbol},{interval},,7"
        self.write(msg)
        # Update the expected field names coming from the API.
        self.request_fieldnames(field_type="Q")

        return None

    def symbol_watch_trades(self, symbol: str) -> None:
        """
        Begins watching a symbol for Level 1 trade updates.

        Args:
            symbol: (str) of the symbol to watch.
        Returns:
            None
        """
        self._symbol = symbol
        msg = f"t{self._symbol}\r\n"
        log.info(f"Start watching symbol {self._symbol} on L1 port.")
        self.write(msg)
        self.request_fieldnames(field_type="Q")

        return None

    def symbol_terminate(self) -> None:
        """
        Terminates watching a symbol for updates or trades.
        """
        if self._port == 5009:
            # L1 port terminate updates.
            msg = f"r{self._symbol}\r\n"
        elif self._port == 9200:
            # L2 port terminate updates.
            msg = f"ROR,{self._symbol}\r\r\n"
        else:
            log.error("Failed to terminate. No L1 or L2 stream connection.")

        self.write(msg)

        return None

    def symbol_refresh(self) -> None:
        """
        Force refreshes a symbol L1 updates or trades stream.
        """
        msg = f"f{self._symbol}\r\n"
        self.write(msg)

        return None

    def check_system_messages(self, messages: list) -> bool:
        """
        Check the messages for the leading "S", which indicates the messages are
        system messages. This is used to indicate whether the messages are
        startup messages from intializing a connection to an L1 / L2 port.

        Args:
            messages: (list) responses from socket connection split on \n.
        Returns:
            (bool) True if the messages contain a leading "S" indicating that
            the messages are system messages.
        """
        is_system_message = False
        for message in messages:
            split_message = message.split(",")
            if split_message[0] == "S":
                is_system_message = True
                break

        return is_system_message

    def check_error(self, messages: list) -> list:
        """
        Check the list of split messages for error codes from the API, which
        typically begin with a capital 'E'. If there is an error, log the error
        and return the messages list excluding the error message.
        """
        idx_delete = None
        for i in range(len(messages)):
            split_message = messages[i].split(",")
            if split_message[0] == "E":
                idx_delete = i
                log.error(f"IQFeed Error: {split_message[1]}")

        if idx_delete is not None:
            del messages[idx_delete]

        return messages

    def check_startup(self, messages: list) -> bool:
        """
        Check the startup messages that are returned when initiating an L1 / L2
        stream. Once connected, IQConnect will deliver five data messages
        separated by a new line character. If the messages are as expected and
        the server is connected, then return True.

        Args:
            messages: (list) responses from initial connection split on \n.
        Returns:
            (bool) whether the server connection was successful or not.
        """
        is_connected = False
        expected_messages = [
            "KEYOK",
            "CUST",
            "IP",
            "SERVER CONNECTED",
            "KEY",
        ]
        # Messages are already split on new line character into a list.
        for message in messages:
            split_message = message.split(",")
            if (
                split_message[1] in expected_messages
                and split_message[1] == "SERVER CONNECTED"
            ):
                is_connected = True

        return is_connected

    def process_admin(self, messages: list) -> pd.DataFrame:
        """
        Process csv data that is returned on the admin port 9300 and return the
        cleaned and separated data as a data frame. Refer to the documentation
        https://www.iqfeed.net/dev/api/docs/AdminSystemMessages.cfm

        Args:
            messages: (list) contains csv messages received from admin socket.
        Returns:
            pd.DataFrame containing the cleaned and separated data.
        """
        # Process CURRENT PROTOCOL, STATS, and CLIENTSTATS differently.
        data_out = list()
        ignore_message_types = ["CURRENT PROTOCOL", "CLIENTSTATS"]
        split_messages = [m.split(",") for m in messages]
        for split_message in split_messages:
            # Ignore protocol messages and client stats.
            if split_message[1] in ignore_message_types:
                continue
            elif split_message[1] == "STATS":
                data_out.append(split_message)

        return pd.DataFrame(data_out)

    def process_stream(self, messages: list) -> pd.DataFrame:
        """
        Process csv data that is returned on the level 1 port 5009 or the level
        2 port 9200 based on the expected fields for the connection. Then return
        cleaned and separated data as a data frame. Refer to the documentation
        https://www.iqfeed.net/dev/api/docs/AdminSystemMessages.cfm

        Args:
            messages: (list) contains csv messages received from L1 / L2.
        Returns:
            pd.DataFrame containing the cleaned and separated data.
        """
        data_out = []
        for message in messages:
            split_message = message.split(",")
            if split_message[0] == "Q":
                del split_message[-1]  # Drop the trailing blank space.
                data_out.append(split_message[1:])

        return pd.DataFrame(data_out, columns=self._update_fieldnames)

    def request_fieldnames(self, field_type: str) -> list:
        """
        There are 21 expected fields for version 6.2. It's good practice to
        set this field expectation when the connection is initialized.

        Args:
            field_type: (str) F is fundamental and Q is summary / update fields.
        Returns:
            (list) of the fieldnames
        """
        return_fields = ["FUNDAMENTAL FIELDNAMES", "CURRENT UPDATE FIELDNAMES"]

        if field_type == "F":
            msg = "S,REQUEST FUNDAMENTAL FIELDNAMES\r\n"
        elif field_type == "Q":
            msg = "S,REQUEST CURRENT UPDATE FIELDNAMES\r\n"
        else:
            log.error("Need to specify field_type 'F' or 'Q'.")

        self.write(message=msg)
        messages = self.read()
        field_names = None
        for message in messages:
            split_message = message.split(",")
            if len(split_message) < 3:
                continue
            elif split_message[1] not in return_fields:
                continue
            else:
                field_names = split_message[2:]

        return field_names

    def select_fieldnames(self, fieldnames: list) -> None:
        """
        Given a list containing desired update / summary fieldnames, set the API
        to only send those fields and update the connection object.

        Args:
            fieldnames: (list) containing the desired fieldnames.
        Returns:
            None
        """
        msg_fields = "S,SELECT UPDATE FIELDS," + ",".join(fieldnames) + "\r\n"
        self.write(message=msg_fields)
        messages = self.read()
        for message in messages:
            split_message = message.split(",")
            if split_message[1] != "CURRENT UPDATE FIELDNAMES":
                continue
            else:
                self._update_fieldnames = split_message[2:]
                self._symbol = split_message[2]

        return None
