import py_compile, sys
try:
    py_compile.compile(r'Data_analytics/src/pipelines/ccf_analysis/analyze_competitors_ccf.py', doraise=True)
    print('OK')
except Exception as e:
    print('ERROR:', e)
    sys.exit(1)
