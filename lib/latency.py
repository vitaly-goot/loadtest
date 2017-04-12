import math
import copy
from ast import literal_eval

class Latency():
    def __init__(self, frame=20, resolution=1000):
        self.count = 0
        self.sum = 0
        self.mean = 0
        self.stddev = 0
        self.min = 2**32
        self.max = 0
        self.variance = 0
        self.frame = frame
        self.resolution = resolution
        self.precision = 1
        while resolution > 1:
            resolution/=10
            self.precision += 1
        self.samples = {}
    
    def from_str(self, s):
        self.__dict__ =  literal_eval(s)

    def to_str(self):
        return str(self.__dict__).replace(' ', '')

    def report(self, distribution=False):
        info = {
            'count':self.count,
            'min': round(self.min,self.precision),
            'max': round(self.max,self.precision),
            'mean': round(self.mean,self.precision),
            'stddev': round(self.stddev,self.precision),
            'sum': round(self.sum,self.precision),
            'variance': round(self.variance,self.precision),
        }
        if distribution: info['dist'] = sorted(self.samples.items())
        return str(info).replace(' ', '')
        
    def add_value(self, time):
        time_ms = int(time*self.resolution)
        frame = time_ms - time_ms % self.frame
        if self.samples.has_key(frame):
            self.samples[frame] += 1
        else:    
            self.samples[frame] = 1

        new_count = self.count + 1
        if self.count < 1:
            new_mean = time
            new_variance = 0
        else: 
            new_mean = self.mean + (time - self.mean) / new_count
            new_variance = self.variance + (time - self.mean) * (time - new_mean)
        if self.count > 1:
            self.stddev = math.sqrt(new_variance / (new_count - 1))
        self.count = new_count
        self.mean = new_mean
        self.variance = new_variance
        self.min = min(self.min, time)
        self.max = max(self.max, time)
        self.sum += time


    def combine(self, other):
        for k, v in other.samples.items():
            for i in range(v):
                self.add_value(k/1000.0)

    def combine_3(self, other):
        if other.count == 0:
            return

        if self.count == 0:
            self.__dict__ = other.__dict__.copy()
            return

        combined_mean = (self.mean * self.count + other.mean * other.count) / (self.count + other.count)
        self.variance = (self.count * (self.variance + (self.mean - combined_mean)**2) + other.count * (other.variance + (other.mean - combined_mean)**2)) / (self.count + other.count)
        if (self.count + other.count) > 1:
            #self.stddev = math.sqrt(self.variance)
            self.stddev = math.sqrt(self.variance / (self.count + other.count - 1))
        self.min = min(self.min, other.min)
        self.max = max(self.max, other.max)
        self.mean = combined_mean
        self.count += other.count
        self.sum += other.sum


    def combine_2(self, other):
        if other.count == 0:
            return

        if self.count == 0:
            self.__dict__ = other.__dict__.copy()
            return

        combined_mean = (self.mean * self.count + other.mean * other.count) / (self.count + other.count)
        print combined_mean
        combined_variance = ((self.variance + self.mean**2)*self.count + (other.variance + other.mean**2)*other.count) / (self.count + other.count) - combined_mean**2
        self.variance += combined_variance
        if (self.count + other.count) > 1:
            self.stddev = math.sqrt(self.variance / (self.count + other.count - 1))
        self.min = min(self.min, other.min)
        self.max = max(self.max, other.max)
        self.mean = combined_mean
        self.count += other.count
        self.sum += other.sum


