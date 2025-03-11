config = {
    "input_dir": "/home/zaman/repos/data_anonymization/J1PROD",
    "output_dir": "/home/sakib/Downloads/d2l_anonymized",
    "anonymize_columns_j1_seu":{
        "APPID":"random_number",
        "ABSENCE_CDE":"random_number",
        "ABSENCE_DESC":"random_address",
        "ABSENCE_CDE_STS":"random_email",
        "JOB_NAME":"random_name",
        "JOB_TIME":"random_date",
        
    },
    "j1_scf_match_tables":[
        'ABSENCE_DEF',
        'ACA_4980H_TRANS_RELIEF_CODES',
        'ACA_MEDIA_WRK',
        'ACCT_CMP_1_DEF'
    ]
}
