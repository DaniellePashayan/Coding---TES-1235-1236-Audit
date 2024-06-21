import pandas as pd
from datetime import datetime, timedelta
from glob import glob
import os
from tqdm import tqdm

def get_outbound_files(path: str, date: datetime):
    date = date.strftime('%m%d%Y')
    criteria = f'*_Outbound_{date}.xlsx'
    files = glob(f'{path}/{criteria}')
    return files

def combine_outbound_files(path: str, min_date: datetime, max_date: datetime):
    current_date = min_date
    all_files = []

    while current_date <= max_date:
        files_for_date = get_outbound_files(path, current_date)
        if files_for_date:  # Check if files are found for the current date
            all_files.extend(files_for_date)
        current_date += timedelta(days=1)  # Move to the next month

    return all_files

def format_outbound_files(df: pd.DataFrame):
    cols_to_keep = ['INVNUM', 'RetrievalStatus', 'RetrievalDescription', 'Reason','TransactionStartDate', 'TransactionEndDate']
    # keep only the columns we need

    df = df[cols_to_keep]
    df = df.reset_index(drop=True)
    
    df['TransactionEndDate'] = pd.to_datetime(df['TransactionEndDate'])
    df['TransactionStartDate'] = pd.to_datetime(df['TransactionStartDate'])
    return df

def get_kav_reports(path: str, min_date: datetime, max_date: datetime):
    """
    Folder structure looks like this:
    KAV Reports
    | 2023/
    | | 2023.01/
    | | | 2023.0101/
    | | | | CBO - TES BOT CPT Change Audit 2023.0101.xlsx
    | 2024/
    | | 2024.01/
    | | | 2024.0101/
    | | | | CBO - TES BOT CPT Change Audit 2024.0101.xlsx
    | 2024.02
    | | 2024.0201/
    | | | CBO - TES BOT CPT Change Audit 2024.0201.xlsx
    | 2024.0227
    | | CBO - TES BOT CPT Change Audit 2024.0227.xlsx
    
    The current week is in the main KAV Reports Folder
    The current month is in the main KAV Reports Folder
    Once the week is over, the folder is moved to the monthly folder
    Once that month is over, the monthly folder is moved to the yearly folder
    """
    min_year = min_date.year
    max_year = max_date.year

    if min_year == max_year:
        years = [min_year]
    else:
        years = [str(year) for year in range(min_year, max_year + 1)]

    # get folders from main dir
    _, main_folders, _ = next(os.walk(path))
    main_folders = [f for f in main_folders if any(str(year) in f for year in years)]

    # go through main_folders and get a list of the folders that have a len of 4
    yearly_folders = [f"{path}/{f}" for f in main_folders if len(f) == 4] 
    monthly_folders = [f"{path}/{f}" for f in main_folders if len(f) == 7] #formatting 2024.01
    daily_folders = [f"{path}/{f}" for f in main_folders if len(f) == 9] #formatting 2024.0101
    rpa_reports = []

    for year_folder in yearly_folders:
        _, month_folders, _ = next(os.walk(year_folder))
        monthly_folders.extend([os.path.join(path,year_folder,f) for f in month_folders])

    for month_folder in monthly_folders:
        rpa_reports.extend(glob(f'{month_folder}/**/CBO - TES BOT CPT Change Audit*.xlsx'))

    for daily_folder in daily_folders:
        rpa_reports.extend(glob(f'{daily_folder}/CBO - TES BOT CPT Change Audit*.xlsx'))
    
    final = pd.concat([pd.read_excel(file) for file in tqdm(rpa_reports)])
    final['Audit Datetime'] = pd.to_datetime(final['AuditDt'] + ' ' + final['AuditTm'])
    
    return final

if __name__ == '__main__':
    # setup
    min_date = datetime(2024, 6, 13)
    max_date = datetime.today()

    outbound_1235_path, outbound_1236_path = 'M:/CPP-Data/Sutherland RPA/Coding/CSE1235','M:/CPP-Data/Sutherland RPA/Coding/CSE1236'
    outbound_paths = [outbound_1235_path, outbound_1236_path]

    kav_path = 'M:/CPP-Data/CBO Westbury Managers/KAV Reports'
    
    all_files = []
    for path in outbound_paths:
        all_files.extend(combine_outbound_files(path, min_date, max_date))
    # Combine all files into a single DataFrame
    df = pd.concat([pd.read_excel(file) for file in all_files])
    outbound_files = format_outbound_files(df)
    
    kav_report = get_kav_reports(kav_path, min_date, max_date)
    
    # merge the two data frames on the rows where the Audit Datetime is between the TransactionStartDate and TransactionEndDate
    df = outbound_files.merge(kav_report, how='left', left_on=['INVNUM'], right_on=['Enc'])
    df = df[(df['Audit Datetime'] >= df['TransactionStartDate']) & (df['Audit Datetime'] <= df['TransactionEndDate'])]

    max_date_str = max_date.strftime('%Y %m %d')
    min_date_str = min_date.strftime('%Y %m %d')

    review = df[df['CPTChange'].isna()]

    os.makedirs('./review', exist_ok=True)

    with pd.ExcelWriter(f'review/{min_date_str} to {max_date_str}.xlsx') as writer:
        review.to_excel(writer, index=False, sheet_name='Review')
        df.to_excel(writer, index=False, sheet_name='All')