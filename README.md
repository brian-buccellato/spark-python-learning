# spark-python-learning
Collection of scripts and snippets from Frank Kane's class

# MacOS Setup Using Homebrew
1. `brew install openjdk@17`
2. `brew install scala`
3. `brew install apache-spark`
4. Optionally, Create a log4j.properties file via
- `cd /opt/homebrew/Cellar/apache-spark/3.5.2/libexec/conf`  (substitute 3.5.2 for the version actually installed – the path may be slightly different on your system.)
- `cp log4j2.properties.template log4j2.properties`
- Edit the log4j2.properties file and change the log level from “info” to “error” on log4j.rootCategory.
