## DON'T SUBMIT TO KITWARE DASHBOARD SERVER!!!


# Dashboard is opened for submissions for a 24 hour period starting at
# the specified NIGHLY_START_TIME. Time is specified in 24 hour format.
SET (CTEST_NIGHTLY_START_TIME "23:00:00 EDT")

# Dart server to submit results (used by client)
IF(CTEST_DROP_METHOD MATCHES http)
#  SET (CTEST_DROP_SITE "public.kitware.com")
  SET (CTEST_DROP_SITE "localhost")
  SET (CTEST_DROP_LOCATION "/cgi-bin/HTTPUploadDartFile.cgi")
ELSE(CTEST_DROP_METHOD MATCHES http)
#  SET (CTEST_DROP_SITE "public.kitware.com")
  SET (CTEST_DROP_SITE "localhost")
  SET (CTEST_DROP_LOCATION "/incoming")
  SET (CTEST_DROP_SITE_USER "ftpuser")
  SET (CTEST_DROP_SITE_PASSWORD "public")
ENDIF(CTEST_DROP_METHOD MATCHES http)

SET (CTEST_TRIGGER_SITE 
       "http://${DROP_SITE}/cgi-bin/Submit-vtk-TestingResults.pl")