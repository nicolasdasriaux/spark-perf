# Setup

## IntelliJ

* Install **IntelliJ IDEA** (https://www.jetbrains.com/idea/download)
* Be sure install the **Scala plugin**

## Hive (Windows only)

:warning: You don't have to install Hadoop to get Hive,
Spark will emulate just enough Hive (and Hadoop) locally for everything to run properly.
But, **on Windows** you will need to install a special utility to emulate Unix style filesystem.

* Download `winutils` \
  [https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe]()

* Find a directory where to install `winutils.exe` \
  such as `C:\development\programs\hadoop-2.7.1` (or whatever directory, just avoid spaces in path)
  
* Copy `winutils.exe` to `bin` sub-directory of `C:\development\programs\hadoop-2.7.1` directory

* Add `HADOOP_HOME` environment variable
  ```
  HADOOP_HOME=C:\development\programs\hadoop-2.7.1
  ```

* Add `%HADOOP_HOME%\bin` to `PATH`

* Launch the following command
  ```
  winutils chmod -R 777 C:\tmp\hive
