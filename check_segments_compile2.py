import py_compile, sys
try:
    py_compile.compile(r'Data_analytics/src/pipelines/review_segments/prepare_reviews_with_segments.py', doraise=True)
    print('OK')
except Exception as e:
    print('ERROR:', e)
    sys.exit(1)
