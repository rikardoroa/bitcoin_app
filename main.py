import logging.config
import sys
from dotenv import load_dotenv
import threading
from tables_db_creation import db_tables
from data_extraction import data_extraction
import logging


load_dotenv()
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('tracer')

#class instance
bulk_request = data_extraction()
average = db_tables()



class main_app:

    #init variables
    def __init__(self):
        self.get_data = bulk_request.get_coin_date
        self.get_data_sk_bulk = bulk_request.get_request_data
        self.get_data_bk_bulk = bulk_request.get_requets_bulk_data
        self.get_average_price = average.average_price
        self.creating_tables = average.creating_tables()

    #main menu for every function execution on console
    def option(self):
        try:
            while True:
                # initial input message for write option in the console
                print("print CTRL - C  to interrupt the program")
                option = input(
                    "Press 1 to generate a single file\n"
                    "Press 2 for generate multiple files \n"
                    "Press 3 menu \n"
                    "Press 4 print usd price average \n"
                    "Press 5 exit:")
                option = int(option)
                # validating the option and choices
                #threading execution
                if isinstance(option, int):
                    if option == 1:
                        thread_one = threading.Thread(target=self.get_data)
                        thread_one.start()
                        thread_one.join()
                        thread_two = threading.Thread(target=self.get_data_sk_bulk)
                        thread_two.start()
                        thread_two.join()
                        thread_three = threading.Thread(target=self.option)
                        thread_three.start()
                        thread_three.join()
                        return thread_one, thread_two, thread_three
                    elif option == 2:
                        thread_one = threading.Thread(target=self.get_data_bk_bulk)
                        thread_one.start()
                        thread_one.join()
                        thread_two = threading.Thread(target=self.option)
                        thread_two.start()
                        thread_two.join()
                        return thread_one, thread_two
                    elif option == 3:
                        thread_three = threading.Thread(target=self.option)
                        thread_three.start()
                        thread_three.join()
                        return thread_three
                    elif option == 4:
                        thread_one = threading.Thread(target=self.get_average_price)
                        thread_one.start()
                        thread_one.join()
                        thread_two = threading.Thread(target=self.option)
                        thread_two.start()
                        thread_two.join()
                        return thread_one, thread_two
                    elif option == 5:
                        sys.exit(0)
                    else:
                        print("invalid option!, try again!")
        # logging messages
        except (ValueError, TypeError):
            logger.error("incorrect value, try again..exiting the program")
        except (KeyboardInterrupt, EOFError):
            logger.error("CTRL - C pressed exiting the program")
            raise sys.exit("CTRL- C PRESSED!")

    def run_tables_creation(self):
        try:
            # running tables and databases
            thread_one = threading.Thread(target=self.creating_tables)
            thread_one.start()
            thread_one.join()
        except (KeyboardInterrupt, EOFError):
            logger.error("CTRL - C pressed exiting the program")
            raise sys.exit("CTRL- C PRESSED!")


if __name__ == "__main__":
    run = main_app()
    run.run_tables_creation()
    run.option()
