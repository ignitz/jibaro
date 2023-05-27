from pyspark.sql import SparkSession
from packaging import version

__all__ = ["JibaroSparkSession"]


class JibaroSession(SparkSession):
    class JibaroBuilder(SparkSession.Builder):
        def __init__(self):
            super().__init__()

        def check_between(self, spark_version, min_version, max_version):
            """
            Check if the spark version is between min_version and max_version

            Parameters
            ----------
            spark_version : str
                Spark version
            min_version : str
                Minimum version
            max_version : str
                Maximum version

            Returns
            -------
            bool
                True if spark_version is between min_version and max_version
            """
            if (
                version.parse(min_version)
                <= version.parse(spark_version)
                < version.parse(max_version)
            ):
                return True
            else:
                return False

        def getOrCreate_three_three(self) -> "JibaroSession":
            """
            Spark 3.3.X
            """
            with self._lock:
                from pyspark.context import SparkContext
                from pyspark.conf import SparkConf

                session = JibaroSession._instantiatedSession
                if session is None or session._sc._jsc is None:
                    if self._sc is not None:
                        sc = self._sc
                    else:
                        sparkConf = SparkConf()
                        for key, value in self._options.items():
                            sparkConf.set(key, value)
                        # This SparkContext may be an existing one.
                        sc = SparkContext.getOrCreate(sparkConf)
                    # Do not update `SparkConf` for existing `SparkContext`, as it's shared
                    # by all sessions.
                    session = JibaroSession(sc)
                for key, value in self._options.items():
                    session._jsparkSession.sessionState().conf().setConfString(
                        key, value
                    )
                session.__class__ = JibaroSession
                return session

        def getOrCreate_three_four(self) -> "JibaroSession":
            """
            Spark 3.4.X
            """
            import os
            from pyspark.context import SparkContext
            from pyspark.conf import SparkConf

            opts = dict(self._options)

            with self._lock:
                if "SPARK_REMOTE" in os.environ or "spark.remote" in opts:
                    with SparkContext._lock:
                        if (
                            SparkContext._active_spark_context is None
                            and JibaroSession._instantiatedSession is None
                        ):
                            url = opts.get(
                                "spark.remote", os.environ.get("SPARK_REMOTE")
                            )

                            if url.startswith("local"):
                                os.environ["SPARK_LOCAL_REMOTE"] = "1"
                                JibaroSession._start_connect_server(url, opts)
                                url = "sc://localhost"

                            os.environ["SPARK_REMOTE"] = url
                            opts["spark.remote"] = url
                            return JibaroSession.builder.config(map=opts).getOrCreate()
                        elif "SPARK_LOCAL_REMOTE" in os.environ:
                            url = "sc://localhost"
                            os.environ["SPARK_REMOTE"] = url
                            opts["spark.remote"] = url
                            return JibaroSession.builder.config(map=opts).getOrCreate()
                        else:
                            raise RuntimeError(
                                "Cannot start a remote Spark session because there "
                                "is a regular Spark session already running."
                            )

                session = JibaroSession._instantiatedSession
                if session is None or session._sc._jsc is None:
                    sparkConf = SparkConf()
                    for key, value in self._options.items():
                        sparkConf.set(key, value)
                    # This SparkContext may be an existing one.
                    sc = SparkContext.getOrCreate(sparkConf)
                    # Do not update `SparkConf` for existing `SparkContext`, as it's shared
                    # by all sessions.
                    session = JibaroSession(sc, options=self._options)
                else:
                    getattr(
                        getattr(session._jvm, "SparkSession$"), "MODULE$"
                    ).applyModifiableSettings(session._jsparkSession, self._options)
                session.__class__ = JibaroSession
                return session

        def getOrCreate(self) -> "JibaroSession":
            from pyspark import __version__ as spark_version

            if self.check_between(spark_version, "3.3.0", "3.4.0"):
                return self.getOrCreate_three_three()
            elif self.check_between(spark_version, "3.4.0", "3.5.0"):
                return self.getOrCreate_three_four()
            else:
                raise NotImplementedError(
                    "Spark version {} not supported".format(spark_version)
                )

    builder = JibaroBuilder()

    @property
    def read(self):
        from jibaro.spark.readwriter import JibaroDataFrameReader

        if hasattr(self, "_wrapped"):
            return JibaroDataFrameReader(self._wrapped)
        else:
            return JibaroDataFrameReader(self)

    @property
    def readStream(self):
        from jibaro.spark.streaming import JibaroDataStreamReader

        if hasattr(self, "_wrapped"):
            return JibaroDataStreamReader(self._wrapped)
        else:
            return JibaroDataStreamReader(self)
