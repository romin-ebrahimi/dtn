from datetime import datetime
from iqfeed import Connection
import logging
import os
import pandas as pd
import subprocess
import time

log = logging.getLogger(__name__)

"""
The Service class is used to launch the IQFeed API service and manage
connections to the API. The Connection class is used for maintaining connections
using TCP.

(port 5009) Stream Level1 is used to get real-time data on instruments and news.
(port 9200) Stream Level2 is used to get extended quotes for instruments. For
    each ECN you can get the best pair of quotes and market depth.
(port 9100) Lookup is used to search for instruments, retrieve historical
    data, get advanced information on news.
(port 9300) Stream Admin is used to gather information about the feed and change
    settings. Use this port to get the current status of feed as well as stats
    about client connections.
(port 9400) Lookup derivatives port for interval bars data.

Params: product, login, and password are provided by DTN. The version is the
    user provided version of the Fractal Gambit product.

Note: messages sent to the DTN IQFeed API typically have a <CR><LF> suffix,
which are the carriage return and new line symbols "\r\n". Additionally, there
are pacing violations where the API will only allow 50 requests in 1 second.
"""


class Service:
    def __init__(
        self,
        product: str = "INSERT_PRODUCT_NAME",
        version: str = "0.0",
        login: str = "USER",
        password: str = "PASS",
    ):
        """
        Args:
            product_id: Product ID given by DTN.
            version: Version of dtn_api app not the version of IQFeed.
            login: IQFeed service login.
            password: IQFeed service password.
        """
        # Allowable ports for connecting to IQFeed service API.
        # Admin port 9300 must be first in any connection sequence.
        self._host = "127.0.0.1"
        self._product = product
        self._version = version
        self._login = login
        self._password = password
        self._connected = False
        # Queue for tracking timestamps of .write() requests made to API.
        self._time_stamp_queue = list()
        # Create admin port connection and ghost level 1 port connection.
        self.connections = {
            "9300": Connection(port=9300, name="admin"),
            "5009": Connection(port=5009, name="ghost"),
        }

    def launch(self):
        """
        Launch iqconnect.exe as a subprocess and initialize silent connections
        to each port. Once the last socket connection is disconnected, the
        IQFeed.exe API will exit. Use no hangup to make sure the process stays
        running in the background.
        """
        wine_prefix = "xvfb-run --listen-tcp -a nohup wine"
        # Use single quotes around iq_path and iqfeed_args when calling wine.
        iq_path = "'/root/.wine/drive_c/Program Files/DTN/IQFeed/iqconnect.exe'"
        iqfeed_args = (
            "'-product {} -version {} -login {} -password {} ".format(
                self._product,
                self._version,
                self._login,
                self._password,
            )
            + "-autoconnect' /S"
        )
        # The /S command is a powershell command to assist with the exe.
        iqfeed_call = wine_prefix + " " + iq_path + " " + iqfeed_args
        log.info("Running {}".format(iqfeed_call))

        if not self._connected:
            self.iqconnect_process = subprocess.Popen(
                iqfeed_call,
                shell=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                preexec_fn=os.setpgrp,
            )
        else:
            log.warning("Not Launching. IQFeed Service is already launched.")

        time.sleep(5)
        start_time = time.time()
        # Create initial connections to the admin port and level 1 stream port.
        # Keep attempting initial connections until time_out is reached.
        while not self._connected and time.time() - start_time <= 30:
            self.connections["9300"].connect()
            self.health_check()
            time.sleep(1)

        if not self._connected:
            log.error("Failed to connect IQFeed API. Try restarting.")
        else:
            log.info("Created initial connections to IQFeed API.")

            msg_client = "S,CLIENTSTATS ON\r\n"
            msg_connect = "S,CONNECT\r\n"

            # Turns on status updates for IQFeed service on the admin port.
            self.connections["9300"].write(msg_client)
            log.info(self.connections["9300"].read())
            # Explicitly tell the API to connect to the DTN server.
            self.connections["9300"].write(msg_connect)
            log.info(self.connections["9300"].read())
            # Connect a dummy connection to the L1 streaming port.
            self.connections["5009"].connect()

            log.info("IQFeed service launched.")

    def connection(
        self,
        port: int,
        name: str = "",
        timeout: int = 7,
    ):
        """
        Create a new connection object, store it in the service object, and
        initialize the connection to the IQFeed API.

        Args:
            port: (int) port number.
            name: (str) optional unique name for the connection.
        Returns:
            None
        """
        if self.connections.get(name) is None:
            self.connections[name] = Connection(
                host=self._host,
                port=port,
                name=name,
                timeout=timeout,
            )
            self.connections[name].connect()
        elif (
            self.connections.get(name) is not None
            and not self.connections[name]._connected
        ):
            # Need to create a new connection object each time. If trying to
            # connect an existing connection object, it will give an error:
            # [Errno 9] Bad file descriptor.
            self.connections[name] = Connection(
                host=self._host,
                port=port,
                name=name,
            )
            self.connections[name].connect()

        if self.connections[name]._connected:
            log.info(f"Connection '{name}' is connected.")
        else:
            log.warning(f"Connection '{name}' failed.")

    def health_check(self) -> bool:
        """
        Check the health status of the iqfeed service. Once connected to the
        Admin port 9300, you will receive an "S,STATS" message every second.
        This stats message can be used to determine if the server is currently
        connected or disconnected as well other statistical information.

        Args:
            None
        Returns:
            (bool) indicating whether the service connection is healthy.
        """
        msg_disconnect = "Not Connected"
        if (
            self.connections.get("9300") is not None
            and self.connections["9300"]._connected
        ):
            msg_frame = self.connections["9300"].read()
            if (
                msg_frame is not None
                and msg_frame.shape[1] > 0
                and msg_frame.iloc[-1, 13] == msg_disconnect
            ):
                self._connected = False
                log.warning(f"IQFeed port 9300 STAT: {msg_frame.iloc[-1, 13]}")
                log.warning("IQFeed service is disconnected from port 9300.")
            else:
                self._connected = True
        elif self.connections.get("9300") is None:
            self._connected = False
            log.warning("Admin port 9300 connection object does not exist.")
        else:
            self._connected = False
            log.warning("IQFeed service is disconnected from port 9300.")

        return self._connected

    def check_requests(self):
        """
        The IQFeed API only allows for 50 requests in a 1 second period. Use
        this method to check the queue of request timestamps, update the queue,
        and pause any requests as needed. This will avoid pacing violations
        before calling the Connection.write() method.
        """
        REQUEST_LIMIT = 50
        # Iterate backwards through the queue, check the seconds elapsed, sleep,
        # and update the queue to exclude requests > 1 second ago.
        for i in range(-1, -len(self._time_stamp_queue), -1):
            diff_time = datetime.now() - self._time_stamp_queue[i]
            if diff_time.total_seconds() >= 1:
                self._time_stamp_queue = self._time_stamp_queue[i:]
                if len(self._time_stamp_queue) >= REQUEST_LIMIT:
                    time.sleep(1)
                break

        self._time_stamp_queue.append(datetime.now())

    def lookup_security_types(self, request_id: str = "") -> pd.DataFrame:
        """
        Query the current list of security types and their codes from the API.

        Args:
            request_id: (str) optional id for making unique requests to the API.

        Returns:
            (dict) of records. Each record identifies a single security type.
            After all listed types are returned by the API, the list is
            terminated with a message in the format:
            "!ENDMSG!,\r\n"
        """
        # Connect to port 9100, query data, then disconnect.
        self.connection(port=9100, name="lookup")

        if request_id == "":
            msg_req = "SST\r\n"
        else:
            msg_req = f"SST,{request_id}\r\n"

        self.check_requests()
        self.connections["lookup"].write(msg_req)
        # This is what is received from the API. The \n char is removed when the
        # data is processed and split on new line characters.
        # --------------------------
        # [RequestID (if specified)],
        # LS,
        # [Security Type ID],
        # [Short Name],
        # [Long Name],\r\n
        col_names = [
            "request_id",
            "sec_type_id",
            "short_name",
            "long_name",
        ]
        if request_id == "":
            del col_names[0]

        sec_types = list()
        queue = True
        while queue:
            try:
                messages = self.connections["lookup"].read()
            except TimeoutError:
                log.warning("Time out exception in lookup_security_types.")
                break

            for message in messages:
                if message == "!ENDMSG!,\r":
                    queue = False
                    break
                elif message == "":
                    continue
                else:
                    split_message = message.split(",")
                    if split_message[-1] == "\r":
                        # Drop carriage return '\r' character.
                        del split_message[-1]

                    if split_message[0] == request_id:
                        # Drop unneccessary request_id element of message.
                        del split_message[0]

                    if split_message[0] == "LS":
                        # Drop unnecessary message_id element of message.
                        del split_message[0]

                    sec_types.append(split_message)

        self.connections["lookup"].disconnect()
        sec_types = pd.DataFrame(sec_types, columns=col_names)

        return sec_types

    def lookup_market_types(self, request_id: str = "") -> pd.DataFrame:
        """
        Query the current list of market types and their codes from the API.

        Args:
            request_id: (str) optional id for making unique requests to the API.

        Returns:
            (dict) of market types. Each record identifies a single listed
            market and its parent market. After all listed markets are returned
            by the API, the list is terminated with a message in the format:
            "!ENDMSG!,\r\n"
        """
        # Connect to port 9100, query data, then disconnect.
        self.connection(port=9100, name="lookup")

        if request_id == "":
            msg_req = "SLM\r\n"
        else:
            msg_req = f"SLM,{request_id}\r\n"

        self.check_requests()
        log.info(f"API request lookup port 9100: {msg_req}")
        self.connections["lookup"].write(msg_req)
        # This is what is received from the API. The \n char is removed when the
        # data is processed and split on new line characters.
        # --------------------------
        # [RequestID (if specified)],
        # LS,
        # [Listed Market ID],
        # [Short Name],
        # [Long Name],
        # [Group ID],
        # [Short Group Name]\r\n
        col_names = [
            "request_id",
            "listed_market_id",
            "short_name",
            "long_name",
            "group_id",
            "short_group_name",
        ]
        if request_id == "":
            del col_names[0]

        mkt_types = list()
        queue = True
        while queue:
            try:
                messages = self.connections["lookup"].read()
            except TimeoutError:
                log.warning("Time out exception in lookup_market_types.")
                break

            for message in messages:
                if message == "!ENDMSG!,\r":
                    queue = False
                    break
                elif message == "":
                    continue
                else:
                    split_message = message.split(",")
                    if split_message[-1] == "\r":
                        # Drop carriage return '\r' character.
                        del split_message[-1]

                    if split_message[0] == request_id:
                        # Drop unneccessary request_id element of message.
                        del split_message[0]

                    if split_message[0] == "LS":
                        # Drop unnecessary message_id element of message.
                        del split_message[0]

                    mkt_types.append(split_message)

        self.connections["lookup"].disconnect()
        mkt_types = pd.DataFrame(mkt_types, columns=col_names)

        return mkt_types

    def lookup_symbol(
        self,
        search_str: str,
        listed_market_id: str = None,
        security_type_id: str = None,
        field_to_search: str = "s",
        request_id: str = "",
        symbol_root: str = None,
    ) -> pd.DataFrame:
        """
        This uses the SBF 'search by filter' functionality that IQFeed offers.
        There are other search methods that are applicable for equities. For
        now, this is designed for FOREX and FUTURES symbol search exclusively.

        Args:
            search_str: (str)
            listed_market_id: (str) the market id returned from market_types.
            security_type_id: (str) security type id from the API lookup method.
                For example, 'FUTURE' security type id is '8'.
            field_to_search: (str) symbols 's' or descriptions with 'd'.
            request_id: (str) optional id for making unique requests to the API.
            symbol_root: (str) optional symbol to filter results further.

        Returns:
            pd.DataFrame of the symbols needed for initiating L1 / L2 streams.
        """
        # Can filter by market or security type.
        if listed_market_id is not None and security_type_id is not None:
            log.error("Can't filter symbols by market_name and security_type.")
        elif listed_market_id is not None:
            filter_type = "e"
            filter_value = listed_market_id
        elif security_type_id is not None:
            filter_type = "t"
            filter_value = security_type_id
        else:
            filter_type, filter_value = "", ""

        # Connect to port 9100, query data, then disconnect.
        self.connection(port=9100, name="lookup")

        # Search string passed to the API is a csv of search elements.
        msg_search = [
            "SBF",
            field_to_search,
            search_str,
            filter_type,
            filter_value,
            request_id,
        ]
        col_names = [
            "request_id",
            "symbol",
            "listed_market_id",
            "sec_type_id",
            "description",
        ]
        if request_id == "":
            del col_names[0]

        msg_search = ",".join(msg_search) + "\r\n"
        self.check_requests()
        log.info("API request lookup port 9100 message: {msg_search}")
        self.connections["lookup"].write(msg_search)

        symbols = list()
        queue = True
        while queue:
            try:
                messages = self.connections["lookup"].read()
            except TimeoutError:
                log.warning(f"Time out exception lookup_symbol {search_str}.")
                break

            for message in messages:
                if message == "!ENDMSG!,\r":
                    queue = False
                    break
                elif message == "":
                    continue
                else:
                    split_message = message.split(",")
                    if split_message[-1] == "\r":
                        # Drop carriage return '\r' character.
                        del split_message[-1]

                    if split_message[0] == request_id:
                        # Drop unneccessary request_id element of message.
                        del split_message[0]

                    if split_message[0] == "LS":
                        # Drop unnecessary message_id element of message.
                        del split_message[0]

                    symbols.append(split_message)

        self.connections["lookup"].disconnect()
        symbols = pd.DataFrame(symbols, columns=col_names)

        if symbol_root is not None:
            # The +3 is for the single character month code and 2 digit year.
            # This filters out symbols that are a superset of the symbol_root.
            symbols = symbols.loc[
                (
                    symbols.symbol.str.contains(symbol_root)
                    & (symbols.symbol.str.len() == len(symbol_root) + 3)
                )
            ]

        return symbols

    def query_historical(
        self,
        symbol: str,
        query_type: str = "trades",
        time_start: str = "",
        time_end: str = "",
        pts_per_send: int = None,
        time_interval: int = 60,
    ) -> pd.DataFrame:
        """
        Given a symbol, query historical data from the IQFeed API. If specifying
        time intervals at least one of time_start or time_end must be provided.
        The default is to exclude time intervals and query all available data.
        1s interval data is only available back 6 months. Anything beyond that
        must be at least 60s intervals.

        Args:
            symbol: (str) of the symbol to query historical data for.
            query_type: (str) one of [trades, interval]
            time_start: (str) optional start time of historical interval. Format
                of time must be CCYYMMDD HHmmSS.
            time_end: (str) optional end time of historical interval. Format of
                time must be CCYYMMDD HHmmSS.
            pts_per_send: (int) optional for queueing data in socket connection.
            time_interval: (int) specifies queried interval size in seconds.
        Returns:
            pd.DataFrame containing the historical data.
        """
        # DatapointsPerSend is the amount of data for IQFeed to queue before
        # sending to socket connection stream.
        if pts_per_send is None:
            pts_per_send = 1024
        if len(time_end) == 0:
            # Default is to query all data up through current.
            time_end = str(datetime.now()).split(".")[0]
            time_end = time_end.replace("-", "").replace(":", "")

        if query_type == "trades":
            # See https://www.iqfeed.net/dev/api/docs/HistoricalviaTCPIP.cfm
            msg = f"HTT,{symbol},{time_start},{time_end},,,,,,"
            # Add DatapointsPerSend to the msg string.
            msg += f"{pts_per_send}\r\n"
            col_names = [
                "time_stamp",
                "last",
                "last_size",
                "total_volume",
                "bid",
                "ask",
                "tick_id",
                "basis_for_last",
                "trade_market_center",
                "trade_conditions",
                "trade_aggressor",
                "day_code",
            ]
        elif query_type == "interval":
            # HIT,symbol,[interval in seconds],time_start,time_end,
            msg = f"HIT,{symbol},{time_interval},{time_start},{time_end},,,,,,"
            # Add DatapointsPerSend to the msg string.
            msg += f"{pts_per_send}\r\n"
            col_names = [
                "time_stamp",
                "high",
                "low",
                "open",
                "close",
                "total_volume",
                "period_volume",
                "number_of_trades",
            ]
        else:
            log.error("query_type must be one of [trades, interval].")

        self.connection(port=9100, name="historical")
        self.check_requests()
        self.connections["historical"].write(msg)

        data_hist = list()
        queue = True
        while queue:
            try:
                messages = self.connections["historical"].read()
            except TimeoutError:
                log.warning(f"Time out exception query_historical {symbol}.")
                break

            for message in messages:
                if message == "!ENDMSG!,\r":
                    queue = False
                    break
                elif message == "":
                    continue
                else:
                    split_message = message.split(",")
                    if split_message[-1] == "\r":
                        del split_message[-1]

                    if split_message[0] == "":
                        # Delete unnecessary request_id part of message.
                        del split_message[0]

                    if split_message[0] == "LH":
                        # Delete unnecessary message_id part of message.
                        del split_message[0]

                    data_hist.append(split_message)

        self.connections["historical"].disconnect()
        data_hist = pd.DataFrame(data_hist, columns=col_names)

        return data_hist

    def shutdown(self):
        """
        Shutdown IQFeed by disconnecting from all existing connections. After
        all connections that were made to the Level 1 Port are disconnected,
        IQConnect will shut down 5s after the last connection is terminated.
        """
        for key in self.connections.keys():
            self.connections[key].disconnect()

        time.sleep(5)
        log.info("Shutdown IQFeed service.")
