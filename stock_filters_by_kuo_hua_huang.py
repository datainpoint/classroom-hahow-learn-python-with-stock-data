import json
import sqlite3
from datetime import date
import numpy as np
import pandas as pd

class StockFilters:
    def __init__(self):
        json_files = ["free_cashflows.json", "gross_profits_and_operating_margins.json",
                           "inventory_turnovers.json", "monthly_revenues.json"]
        csv_files = ["listing_dates.csv", "market_caps.csv",
                          "top_ranking_stocks.csv"]
        db_file = "trading_volumes.db"
        self.imported_files = dict()
        for json_file in json_files:
            with open(json_file, "r") as f:
                self.imported_files[json_file] = json.load(f)
        for csv_file in csv_files:
            self.imported_files[csv_file] = pd.read_csv(csv_file)
        con = sqlite3.connect(db_file)
        tables = ["tickers", "volumes"]
        for table in tables:
            self.imported_files[f"trading_volumes_{table}"] = pd.read_sql(f"""SELECT * FROM {table};""", con)
        con.close()
    def mission_two(self):
        top_ranking_stocks_csv = self.imported_files["top_ranking_stocks.csv"]
        stock_names = top_ranking_stocks_csv["stock"]
        is_ky_registry = stock_names.str.contains("-KY")
        is_not_ky_registry = ~is_ky_registry
        df = pd.DataFrame()
        df["ticker"] = top_ranking_stocks_csv["ticker"]
        df["mission_two"] = is_not_ky_registry
        return df
    def mission_three(self):
        listing_dates_csv = self.imported_files["listing_dates.csv"]
        today = date.today()
        today_ser = pd.to_datetime(today)
        listing_date_ser = pd.to_datetime(listing_dates_csv["listing_date"])
        listing_days = today_ser - listing_date_ser
        listing_days = listing_days.dt.days
        listing_date_notna = listing_dates_csv["listing_date"].notna()
        days_of_two_years = 365*2
        is_listing_days_over_two_years = listing_days > days_of_two_years
        conditions_intersected = listing_date_notna & is_listing_days_over_two_years
        df = pd.DataFrame()
        df["ticker"] = listing_dates_csv["ticker"]
        df["mission_three"] = conditions_intersected
        return df
    def mission_four(self):
        gross_profits_and_operating_margins_json = self.imported_files["gross_profits_and_operating_margins.json"]
        is_profitable = list()
        for data in gross_profits_and_operating_margins_json:
            ticker = data["ticker"]
            gross_profit = data["gross_profit"]
            net_income = data["net_income"]
            if net_income is None:
                dict_to_append = {
                    "ticker": ticker,
                    "mission_four": False
                }
                is_profitable.append(dict_to_append)
            else:
                list_net_income_values = list(net_income.values())
                arr = np.array(list_net_income_values)
                arr_is_negative = arr < 0
                arr_sum = arr_is_negative.sum()
                dict_to_append = {
                    "ticker": ticker,
                    "mission_four": not bool(arr_sum)
                }
                is_profitable.append(dict_to_append)
        df = pd.DataFrame(is_profitable)
        return df
    def mission_five(self):
        gross_profits_and_operating_margins_json = self.imported_files["gross_profits_and_operating_margins.json"]
        is_operating_margin_not_declining = list()
        for data in gross_profits_and_operating_margins_json:
            ticker = data["ticker"]
            operating_margin = data["operating_margin"]
            if operating_margin is None:
                dict_to_append = {
                    "ticker": ticker,
                    "mission_five": False
                }
                is_operating_margin_not_declining.append(dict_to_append)
            else:
                list_operating_margin_values = list(operating_margin.values())
                arr = np.array(list_operating_margin_values)
                arr_reversed = arr[::-1]
                diff_arr_reversed = np.diff(arr_reversed)
                arr_is_geq_zero = diff_arr_reversed >= 0
                arr_sum = arr_is_geq_zero.sum()
                dict_to_append = {
                    "ticker": ticker,
                    "mission_five": bool(arr_sum)
                }
                is_operating_margin_not_declining.append(dict_to_append)
        df = pd.DataFrame(is_operating_margin_not_declining)
        return df
    def mission_six(self):
        free_cashflows_json = self.imported_files["free_cashflows.json"]
        is_not_negative_free_cashflow_list = list()
        for data in free_cashflows_json:
            ticker = data["ticker"]
            free_cashflow = data["free_cashflow"]
            if free_cashflow is None:
                dict_to_append = {
                    "ticker": ticker,
                    "mission_six": False
                }
                is_not_negative_free_cashflow_list.append(dict_to_append)
            else:
                free_cashflow_values = free_cashflow.values()
                sum_free_cashflow = sum(free_cashflow_values)
                is_not_negative = sum_free_cashflow >= 0
                dict_to_append = {
                    "ticker": ticker,
                    "mission_six": is_not_negative
                }
                is_not_negative_free_cashflow_list.append(dict_to_append)
        df = pd.DataFrame(is_not_negative_free_cashflow_list)
        return df
    def mission_seven(self):
        monthly_revenues_json = self.imported_files["monthly_revenues.json"]
        is_revenue_yoy_positive_list = list()
        for data in monthly_revenues_json:
            ticker = data["ticker"]
            monthly_revenue = data["monthly_revenue"]
            if monthly_revenue is None:
                dict_to_append = {
                    "ticker": ticker,
                    "mission_seven": False
                }
                is_revenue_yoy_positive_list.append(dict_to_append)
            else:
                monthly_revenue_list = list(data["monthly_revenue"].values())
                arr = np.array(monthly_revenue_list)
                # Fancy indexing
                current_period = arr[[0, 2, 4]]
                previous_period = arr[[1, 3, 5]]
                # Slicing
                current_period = arr[0::2]
                previous_period = arr[1::2]
                # Element-wise
                differences = current_period - previous_period
                is_not_positive = differences <= 0
                is_not_positive_sum = is_not_positive.sum()
                revenue_yoy_is_positive = not bool(is_not_positive_sum) # 0: True; > 0: False
                dict_to_append = {
                    "ticker": ticker,
                    "mission_seven": revenue_yoy_is_positive
                }
                is_revenue_yoy_positive_list.append(dict_to_append)
        df = pd.DataFrame(is_revenue_yoy_positive_list)
        return df
    def mission_eight(self):
        market_caps_csv = self.imported_files["market_caps.csv"]
        market_cap = market_caps_csv["market_cap"]
        first_quantile = market_cap.quantile(0.25)
        bool_index = market_cap > first_quantile
        df = pd.DataFrame()
        df["ticker"] = market_caps_csv["ticker"]
        df["mission_eight"] = bool_index
        return df
    def mission_nine(self):
        inventory_turnovers_json = self.imported_files["inventory_turnovers.json"]
        inventory_turnover_is_not_negative_list = list()
        for data in inventory_turnovers_json:
            ticker = data["ticker"]
            inventory_turnover = data["inventory_turnover"]
            if inventory_turnover is None:
                dict_to_append = {
                    "ticker": ticker,
                    "mission_nine": False
                }
                inventory_turnover_is_not_negative_list.append(dict_to_append)
            elif None in inventory_turnover.values():
                dict_to_append = {
                    "ticker": ticker,
                    "mission_nine": False
                }
                inventory_turnover_is_not_negative_list.append(dict_to_append)
            else:
                list_inventory_turnover = list(inventory_turnover.values())
                arr = np.array(list_inventory_turnover)
                delta = np.diff(arr)
                is_not_negative = delta >= 0
                is_not_negative_sum = is_not_negative.sum()
                dict_to_append = {
                    "ticker": ticker,
                    "mission_nine": bool(is_not_negative_sum)
                }
                inventory_turnover_is_not_negative_list.append(dict_to_append)
        df = pd.DataFrame(inventory_turnover_is_not_negative_list)
        return df
    def mission_ten(self):
        tickers = self.imported_files["trading_volumes_tickers"]
        volumes = self.imported_files["trading_volumes_volumes"]
        average_trading_volumes = volumes.groupby("ticker_id")["volume"].mean().reset_index()
        average_trading_volumes_with_tickers = pd.merge(average_trading_volumes, tickers, left_on="ticker_id", right_on="id")
        average_trading_volumes_q25 = average_trading_volumes_with_tickers["volume"].quantile(0.25)
        is_traded_in_high_volume = average_trading_volumes_with_tickers["volume"] > average_trading_volumes_q25
        df = pd.DataFrame()
        df["ticker"] = tickers["ticker"].values
        df["mission_ten"] = is_traded_in_high_volume.values
        return df
    def get_wide_columns(self):
        top_ranking_stocks_csv = self.imported_files["top_ranking_stocks.csv"]
        ticker_stock_and_recored_on = top_ranking_stocks_csv[["ticker", "stock", "recorded_on"]]
        mission_two_df = self.mission_two()
        mission_three_df = self.mission_three()
        mission_four_df = self.mission_four()
        mission_five_df = self.mission_five()
        mission_six_df = self.mission_six()
        mission_seven_df = self.mission_seven()
        mission_eight_df = self.mission_eight()
        mission_nine_df = self.mission_nine()
        mission_ten_df = self.mission_ten()
        merged_df = pd.merge(ticker_stock_and_recored_on, mission_two_df)
        merged_df = pd.merge(merged_df, mission_three_df)
        merged_df = pd.merge(merged_df, mission_four_df)
        merged_df = pd.merge(merged_df, mission_five_df)
        merged_df = pd.merge(merged_df, mission_six_df)
        merged_df = pd.merge(merged_df, mission_seven_df)
        merged_df = pd.merge(merged_df, mission_eight_df)
        merged_df = pd.merge(merged_df, mission_nine_df)
        merged_df = pd.merge(merged_df, mission_ten_df)
        return merged_df
    def get_filtered_stocks(self):
        wide_columns = self.get_wide_columns()
        conditions_intersected = wide_columns["mission_two"] & wide_columns["mission_three"] & \
                                 wide_columns["mission_four"] & wide_columns["mission_five"] & \
                                 wide_columns["mission_six"] & wide_columns["mission_seven"] & \
                                 wide_columns["mission_eight"] & wide_columns["mission_nine"] & \
                                 wide_columns["mission_ten"]
        filtered_wide_columns = wide_columns[conditions_intersected]
        filtered_stocks = filtered_wide_columns[["ticker", "stock", "recorded_on"]]
        return filtered_stocks