import threading
import person_table_streaming
import weather_data_streeming


def start_person():
    person_table_streaming.main()
def start_weather():
    weather_data_streeming.main()


if __name__ == "__main__":

    t1 = threading.Thread(target=start_person)
    t2 = threading.Thread(target=start_weather)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
