from pyspark.sql.streaming.listener import StreamingQueryListener

"""
Listen to Stream events
"""
class StreamListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print("Query started: " + event.id)

    def onQueryProgress(self, event):
        print("Query terminated: " + event.id)

    def onQueryTerminated(self, event):
        print("Query made progress: " + event.progress)