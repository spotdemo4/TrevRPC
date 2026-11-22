# The QUIC stack logs every packet at debug level; warnings and errors still
# show.
Logger.configure(level: :warning)
ExUnit.start()
