Map Reduce Sleep Job
-----------------------------

This job does nothing but start mappers and reducer and ask them to sleep for N milliseconds.  This is good when testing schedulers.

It doesn't read any data and the output folder will be removed when the job is over.


Example:
SleepJob <numberOfMappers> <numberOfRecuers> <mapperSleepTime> <reducerSleepTime> <tmpOutdir>
or
SleepJob 10 10 10000 10000 tmp/output