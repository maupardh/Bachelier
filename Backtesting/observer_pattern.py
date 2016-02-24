

class Observable:
    def __init__(self):
        self.__observers = []

    def register_observer(self, observer):
        self.__observers.append(observer)
    
    def notify_observers(self, *args, **kwargs):
        for observer in self.__observers:
            observer.notify(self, *args, **kwargs)
 
 
class Observer:
    def __init__(self):
        self.observables = []

    def register_observable(self, observable):
        self.observables.append(observable)
        observable.register_observer(self)

    def notify(self, observable, *args, **kwargs):
        pass
