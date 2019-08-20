# Setup

## Installing IntelliJ

* Install **IntelliJ IDEA** (https://www.jetbrains.com/idea/download)
* Be sure install the **Scala plugin** using **Plugins** section of **Settings**

## Installing JDK

* Install at least JDK 8

## Allowing Hive emulation to work (on Windows)

:warning: You don't have to install Hadoop to get Hive,
Spark will emulate just enough Hive (and Hadoop) locally for everything to run properly.
But, **on Windows** you will need to install a special utility to emulate Unix style filesystem.

* Download `winutils` \
  [https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe]()

* Find a directory where to install `winutils.exe` \
  such as `C:\development\programs\hadoop-2.7.1` (or whatever directory, just :warning: avoid spaces in path)
  
* Copy `winutils.exe` to `bin` sub-directory of `C:\development\programs\hadoop-2.7.1` directory

* Add `HADOOP_HOME` environment variable
  ```
  HADOOP_HOME=C:\development\programs\hadoop-2.7.1
  ```

* Add `%HADOOP_HOME%\bin` to `PATH`

* Launch the following command
  ```
  winutils chmod -R 777 C:\tmp\hive

## Cloning the project

* Use `git clone` to clone the project from the repository

:warning: On Windows, it is strongly advised not to have spaces in the path to your project directory.

## Opening the project with IntelliJ

* Just open the directory in IntelliJ
* **Import Project from sbt** dialog box should display,
  here it is recommended to check **for imports** and **for builds** next to **Use sbt shell:**.
