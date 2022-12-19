import runpy
import sys

modules = [
    "./lab100_export/exportWildfiresApp.py",
    "./lab200_feed_delta/feedDeltaLakeApp.py",
    "./lab210_analytics_dept/meetingsPerDepartmentApp.py",
    "./lab220_analytics_author/meetingsPerOrganizerApp.py",
    "./lab250_feed_delta_population/feedFrancePopDeltaLakeApp.py",
    "./lab900_append_primary_key/appendDataJdbcPrimaryKeyApp.py",
]

if __name__ == '__main__':

    for i, module in enumerate(modules):
        print("{0} {1}".format(i, module))
    print("")

    test_text = input("Введите вариант : ")
    module_number = int(test_text)

    runpy.run_path(modules[module_number], run_name='__main__')
