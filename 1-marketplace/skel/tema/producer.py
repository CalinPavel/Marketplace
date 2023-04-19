"""
This module represents the Producer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

import time
from threading import Thread


class Producer(Thread):
    """
    Class that represents a producer.
    """

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):
        """
        Constructor.

        @type products: List()
        @param products: a list of products that the producer will produce

        @type marketplace: Marketplace
        @param marketplace: a reference to the marketplace

        @type republish_wait_time: Time
        @param republish_wait_time: the number of seconds that a producer must
        wait until the marketplace becomes available

        @type kwargs:
        @param kwargs: other arguments that are passed to the Thread's __init__()
        """

        self.products= products
        self.marketplace = marketplace
        self.republish_wait_time = republish_wait_time
        self.id = self.marketplace.register_producer()

        Thread.__init__(self,**kwargs)

        pass

    def run(self):

        while True:
            for i in range(len(self.products)):
                j=0
                while j < self.products[i][1]:
                    
                    check = self.marketplace.publish(str(self.id) , self.products[i][0])
                    if check == 1:
                        time.sleep(self.products[i][2])
                        j +=1
                    else:
                        time.sleep(self.republish_wait_time)

