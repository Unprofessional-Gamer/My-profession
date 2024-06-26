def is_last_working_day_of_month(execution_date, test_date=None):
    if test_date:
        execution_date = test_date
    else:
        execution_date = execution_date or datetime.now()

    year = execution_date.year
    month = execution_date.month
    last_day_of_month = calendar.monthrange(year, month)[1]
    last_day_date = datetime(year, month, last_day_of_month)

    while last_day_date.weekday() > 4:  # 0=Monday, 4=Friday
        last_day_date -= timedelta(days=1)

    return execution_date.date() == last_day_date.date()

def check_last_working_day(**context):
    execution_date = context['execution_date']
    test_date = datetime.now()  # Use current date for testing
    if is_last_working_day_of_month(execution_date, test_date=test_date):
        return 'start_tasks'
    else:
        return 'end'


schedule_interval='0 14 25-31 * *'

