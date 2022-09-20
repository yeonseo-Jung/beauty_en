import pandas as pd
import datetime


def main():
    log_df = pd.read_csv('Glamai작업로그파일.csv')

    dict_info = {'마지막 작업일자': log_df.iloc[-1]['작업 시작일자'],
                 '작업 시작일자': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    log_df = log_df.append(dict_info, ignore_index=True)

    log_df.to_csv('Glamai작업로그파일.csv', index=False)