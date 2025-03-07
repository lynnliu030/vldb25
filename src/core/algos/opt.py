import pandas as pd
from src.core.algos.solver import Algorithm
from typing import Tuple, List


class MPHC(Algorithm):
    """
    OPHR algorithm implementation.
    """

    recursion_counter = 0

    def max_prefix_hit_cnt(
        self, df: pd.DataFrame, prefix: str = "", early_stop: int = 4, select_max_group: bool = False
    ) -> Tuple[pd.DataFrame, int, List[List[str]]]:
        self.__class__.recursion_counter += 1

        # Only one row left
        if df.shape[0] <= 1:
            return df, 0, [list(df.columns)]

        # Only one column left
        if df.shape[1] == 1:
            groups = self.get_groups_values(df)
            prefix_hit_cnt = sum((self.calculate_length(value)) * (count - 1) for value, count in groups.items())
            column_orders = [list(df.columns)] * df.shape[0]
            return df, prefix_hit_cnt, column_orders

        max_score = -1
        best_df = df
        best_column_orders = []

        for col in df.columns:
            grouped_values = self.get_groups_values(df[[col]])
            grouped_values = {k: v for k, v in grouped_values.items() if v >= early_stop}

            for value, _ in grouped_values.items():
                rows = df[df[col] == value]
                df_without_rows = self.drop_rows(df, rows.index)
                df_without_col = self.drop_col(rows, col)

                # phc: prefix hit count
                tot_prefix = prefix + str(value)
                if len(df_without_rows) > 0:
                    df_without_rows_ordered, df_without_rows_phc, _ = self.max_prefix_hit_cnt(
                        df_without_rows, "", early_stop=early_stop, select_max_group=select_max_group
                    )
                else:
                    df_without_rows_ordered = df_without_rows
                    df_without_rows_phc = 0

                if df_without_col.shape[1] > 0:
                    df_without_col_ordered, df_without_col_phc, _ = self.max_prefix_hit_cnt(
                        df_without_col, tot_prefix, early_stop=early_stop, select_max_group=select_max_group
                    )
                else:
                    df_without_col_ordered = df_without_col
                    df_without_col_phc = 0

                curr_phc = self.calculate_length(value) * (rows.shape[0] - 1)
                score = df_without_rows_phc + df_without_col_phc + curr_phc

                if score > max_score:
                    max_score = score
                    best_df = pd.concat([rows[[col]], df_without_col_ordered], axis=1)
                    best_df.columns = range(best_df.shape[1])
                    df_without_rows_ordered.columns = range(df_without_rows_ordered.shape[1])
                    best_df = pd.concat([best_df, df_without_rows_ordered], axis=0)  # concat rest of the rows

        if max_score == -1:
            max_score = 0
            best_column_orders = [list(df.columns)] * df.shape[0]

        return best_df, max_score, best_column_orders

    def reorder(self, df: pd.DataFrame, early_stop: int = 2, select_max_group: bool = False) -> Tuple[pd.DataFrame, List[List[str]]]:
        ordered_df, _, _ = self.max_prefix_hit_cnt(df, early_stop=early_stop)
        assert ordered_df.shape == df.shape
        return ordered_df
