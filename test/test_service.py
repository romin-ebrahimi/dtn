from datetime import datetime, timedelta
import iqfeed.config as config
from iqfeed import Connection
from iqfeed import Service
import time
import unittest

"""
Test Service and Connection in a single module because the IQFeed API has
issues with starting the service twice in quick succession for unit testing.
"""


class TestService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up the IQFeed service for all of the unit test methods.
        """
        cls.iqfeed_service = Service()
        cls.iqfeed_service.launch()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        time.sleep(5)  # Necessary pause.

    def test_00_launch(self):
        """
        IQFeed service is launched before test case. Assert that the admin port
        9300 and L1 port 5009 are both connected.
        """
        self.assertTrue(self.iqfeed_service.connections["9300"]._connected)
        self.assertTrue(self.iqfeed_service.connections["5009"]._connected)

    def test_01_health_check_connected(self):
        self.assertTrue(self.iqfeed_service.health_check())

    def test_02_connect(self):
        """
        Test creating a single connection to the IQFeed Service L1 stream port.
        """
        test_connection = Connection(
            host="127.0.0.1",
            port=5009,
            name="test_connection",
        )
        test_connection.connect()
        self.assertTrue(test_connection._connected)
        test_connection.disconnect()

    def test_03_disconnect(self):
        """
        Test the disconnection method by disconnecting a test connection.
        """
        test_connection = Connection(
            host="127.0.0.1",
            port=5009,
            name="test_connection",
        )
        test_connection.connect()
        test_connection.disconnect()
        self.assertFalse(test_connection._connected)

    def test_04_health_check_disconnected(self):
        self.iqfeed_service.connections["9300"].disconnect()
        self.assertFalse(self.iqfeed_service.health_check())

    def test_05_lookup_security_types(self):
        subset_dict = {
            "sec_type_id": "8",
            "short_name": "FUTURE",
            "long_name": "Future",
        }
        sec_types = self.iqfeed_service.lookup_security_types()

        # Assert that subset dict is a subset of the sec_types dict.
        self.assertTrue(subset_dict in sec_types.to_dict("records"))

    def test_06_lookup_market_types(self):
        subset_dict = {
            "listed_market_id": "13",
            "short_name": "CBOE",
            "long_name": "Chicago Board Options Exchange",
        }
        mkt_types = self.iqfeed_service.lookup_market_types()
        mkt_types = mkt_types[["listed_market_id", "short_name", "long_name"]]

        # Assert that subset dict is a subset of the mkt_types dict.
        self.assertTrue(subset_dict in mkt_types.to_dict("records"))

    def test_07_lookup_symbol_forex(self):
        """
        Gets the necessary sec_type_id for FOREX before querying the symbols
        from the lookup port 9100.
        """
        subset_dict = {
            "symbol": "USDJPY.FXCM",
            "listed_market_id": "74",
            "sec_type_id": "16",
            "description": "FXCM USD JPY SPO",
        }
        sec_types = self.iqfeed_service.lookup_security_types()
        # Get the security type code and search for FOREX symbols.
        security_type_id = sec_types.loc[
            sec_types.short_name == "FOREX", "sec_type_id"
        ].to_string(header=False, index=False)

        symbol_fx = self.iqfeed_service.lookup_symbol(
            search_str="USDJPY",
            security_type_id=security_type_id,
        )

        self.assertTrue(subset_dict in symbol_fx.to_dict("records"))

    def test_08_lookup_symbol_futures(self):
        """
        Gets the necessary sec_type_id for FUTURES before querying the symbols
        from the lookup port 9100.
        """
        search_str = "E-MINI S&P 500"
        sec_types = self.iqfeed_service.lookup_security_types()
        # Get the security type code and search for FUTURES symbols.
        security_type_id = sec_types.loc[
            sec_types.short_name == "FUTURE", "sec_type_id"
        ].to_string(header=False, index=False)

        # Search descriptions for E-Mini S&P 500 futures contracts.
        symbol_ft = self.iqfeed_service.lookup_symbol(
            search_str=search_str,
            security_type_id=security_type_id,
            field_to_search="d",
        )

        self.assertTrue(search_str in symbol_ft.description[0])

    def test_09_check_symbols(self):
        """
        Iterate through all of the symbols stored in the global config.py
        module, assert that each one is searchable through the IQFeed API.
        """
        sec_types = self.iqfeed_service.lookup_security_types()
        searchable_array = []
        for sec_type, symbol_list in config.SECURITIES.items():
            security_type_id = sec_types.loc[
                sec_types.short_name == sec_type, "sec_type_id"
            ].to_string(header=False, index=False)

            if sec_type == "FUTURE":
                # FUTURE triplet = (symbol, listed_market_id, description)
                for symbol, listed_market_id, description in symbol_list:
                    print(f"Symbol search {symbol}")
                    # Search by symbol and filter to target root symbol. Target
                    # symbol length is the root + 3 for the month and year code.
                    # For example, @ES is the root and + 3 for the target MYY.
                    search_result = self.iqfeed_service.lookup_symbol(
                        search_str=symbol[:-2],
                        security_type_id=security_type_id,
                        field_to_search="s",
                        symbol_root=symbol[:-2],
                    )
                    # Filter results down to the listed_market_id.
                    search_result = search_result[
                        search_result.listed_market_id == listed_market_id
                    ]
                    # Drop all results with '#', which represents front months
                    # back adjusted continuous contracts.
                    search_result = search_result[
                        ~search_result.symbol.str.contains("#")
                    ]
                    # Subset to contract symbols that match the expected length.
                    search_result = search_result[
                        search_result.symbol.str.len() == (len(symbol[:-2]) + 3)
                    ]
                    # Drop all results with an empty 'description'.
                    search_result = search_result[
                        search_result.description.str.len() > 0
                    ]

                    # Pull the 2 digit year to the front of the string for
                    # proper sorting resulting in @ESH24, @ESM24, ... instead of
                    # the default sorting yielding @ESH24, @ESH25, ...
                    search_result["sort_column"] = (
                        search_result.symbol.str[-2:]
                        + search_result.symbol.str[:-2]
                    )
                    search_result.sort_values(
                        by="sort_column",
                        ascending=True,
                        inplace=True,
                        ignore_index=True,
                    )
                    if len(search_result) > 0:
                        searchable_array.append(True)
                    else:
                        searchable_array.append(False)
            else:
                continue

        self.assertTrue(all(searchable_array))

    def test_10_stream_symbol(self):
        """
        Connect to the L1 port, initialize a stream, and assert that the data
        received is as expected.
        """
        symbol = "@ES#C"
        self.iqfeed_service.connection(port=5009, name="L1_test")
        self.iqfeed_service.connections["L1_test"].symbol_watch(symbol)
        time.sleep(60)  # Pause and allow some data to come through.
        stream_data = self.iqfeed_service.connections["L1_test"].read()
        colnames = self.iqfeed_service.connections["L1_test"]._update_fieldnames
        self.iqfeed_service.connections["L1_test"].symbol_terminate()
        self.iqfeed_service.connections["L1_test"].disconnect()
        self.assertTrue(list(stream_data.columns) == colnames)

    def test_11_stream_interval(self):
        """
        Connect to the L1 port, initialize a stream for interval bars, and
        assert that the data receive is as expected.
        """
        symbol = "@ES#C"
        self.iqfeed_service.connection(port=5009, name="test_interval")
        self.iqfeed_service.connections["test_interval"].symbol_watch_interval(
            symbol=symbol,
            interval=10,
        )
        time.sleep(30)  # Pause and allow some data to come through.
        stream_data = self.iqfeed_service.connections["test_interval"].read()
        self.assertEqual(
            list(stream_data.columns),
            self.iqfeed_service.connections["test_interval"]._update_fieldnames,
            "Data received is not as expected.",
        )

    def test_12_query_historical(self):
        """
        Query historical data for a single symbol and test that data returned by
        the API is as expected.
        """
        symbol = "@ES#C"
        time_start = datetime.today() - timedelta(days=10)
        time_start = str(time_start).replace("-", "")[:8] + " 000000"
        data_historical = self.iqfeed_service.query_historical(
            symbol=symbol,
            query_type="interval",
            time_start=time_start,
            pts_per_send=10,
        )

        self.assertTrue(len(data_historical) > 0)

    def test_13_shutdown(self):
        """
        Shutdown the IQFeed service and assert that admin port is disconnected.
        """
        self.iqfeed_service.shutdown()
        self.assertFalse(self.iqfeed_service.connections["9300"]._connected)


if __name__ == "__main__":
    unittest.main()
