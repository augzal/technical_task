import unittest
import datetime
from main import filter_data, join_columns, add_column, convert_to_iso, select_cols


class TestDocumnetFormatter(unittest.TestCase):
    def setUp(self):
        self.data = [
            ['col1', 'col2', 'col3'],
            [1, 2, 3],
            ['a', 'b', '6/25/2001'],
            ['a', 2, '8/50/2010']
        ]

    def test_filter_data_int(self):
        filtered_data = filter_data(self.data, col_name='col2', value=2)
        expected_data = [
            ['col1', 'col2', 'col3'],
            [1, 2, 3],
            ['a', 2, '8/50/2010']
        ]
        self.assertEqual(filtered_data, expected_data)

    def test_filter_data_str(self):
        filtered_data = filter_data(self.data, col_name='col1', value='a')
        expected_data = [
            ['col1', 'col2', 'col3'],
            ['a', 'b', '6/25/2001'],
            ['a', 2, '8/50/2010']
        ]
        self.assertEqual(filtered_data, expected_data)

    def test_filter_data_empty(self):
        filtered_data = filter_data(self.data, col_name='col1', value='Not existing')
        expected_data = [
            ['col1', 'col2', 'col3']
        ]
        self.assertEqual(filtered_data, expected_data)

    def test_join_columns(self):
        joined_col = join_columns(self.data, 'col1', 'col2', ' ')
        expected_col = ['1 2', 'a b', 'a 2']
        self.assertEqual(joined_col, expected_col)

    def test_add_column(self):
        values = ['n_1', 2, 'new_val']
        new_data = add_column(self.data, 'new_col', values)
        expected_data = [
            ['col1', 'col2', 'col3', 'new_col'],
            [1, 2, 3, 'n_1'],
            ['a', 'b', '6/25/2001', 2],
            ['a', 2, '8/50/2010', 'new_val']
        ]
        self.assertEqual(new_data, expected_data)

    def test_convert_to_iso(self):
        date_values = convert_to_iso(self.data, 'col3')
        self.assertEqual(date_values, [None, datetime.datetime(2001, 6, 25), None])

    def test_select_cols(self):
        new_data = select_cols(self.data, ['col1', 'col2'])
        expected_data = [
                ['col1', 'col2'],
                [1, 2],
                ['a', 'b'],
                ['a', 2]
            ]
        self.assertEqual(new_data, expected_data)
