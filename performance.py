import tracemalloc
import time
class Performance:
    def __init__(self):
        self.name = "performance"
        
    def tracing_start(self):
        """docstring"""
        tracemalloc.stop()
        print("nTracing Status :", tracemalloc.is_tracing())
        tracemalloc.start()
        print("Tracing Status :", tracemalloc.is_tracing())

    def tracing_memory(self):
        """docstring"""
        first_size, first_peak = tracemalloc.get_traced_memory()
        peak = first_peak/(1024*1024)
        print("Peak Size in MB -", peak)
        return peak


    # class Bar():

        # foo = Foo()
        # param = something

        # @foo.decorate(param)
        # def func(self):
            # # do something
            
    def performance(self,f):
        def decor(*args, **kwargs):
            print()
            self.tracing_start()
            start = time.time()
            result = f(*args, **kwargs)
            end = time.time()
            elapsed = (end-start)*1000
            print("time elapsed {} milli seconds".format(elapsed))
            peak = self.tracing_memory()
            #put time results in a file
            file1 = open("performance.txt", "a")
            writethis="|"+str(round(elapsed,2))+"|"+str(round(peak,2))+"\n"
            file1.write(writethis)
            file1.close()
            return result
        return decor