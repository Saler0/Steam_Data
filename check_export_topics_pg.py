import py_compile, sys
try:
    py_compile.compile(r'Data_analytics/scripts/export_topics_by_experience_to_postgres.py', doraise=True)
    print('OK')
except Exception as e:
    print('ERROR:', e)
    sys.exit(1)
