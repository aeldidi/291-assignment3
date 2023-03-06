import matplotlib.pyplot as plt
import numpy as np
import os.path
from os import path
import csv

def main():
    questions = ["Q1","Q2","Q3","Q4"]
    for question in questions:
        print(parser(question))
        plot(question)


def parser(question_num):
    # {DB:[[User-Optimized], [Self-Optimized], [Uninformed]]}
    data = {"SmallDB":[[],[],[]],
            "MediumDB":[[],[],[]],
            "LargeDB":[[],[],[]]}
    # Parses output data into data dictionary
    files = ["User-Optimized.csv", "Self-Optimized.csv", "Uninformed.csv"]
    try:
        for index, element in enumerate(files):
            with open(f"{question_num}-{element}") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        line_count = 1
                    else:
                        data["SmallDB"][index].append(float(row[0]))
                        data["MediumDB"][index].append(float(row[1]))
                        data["LargeDB"][index].append(float(row[2]))
                # Takes average of all test runs for each scenario
                data["SmallDB"][index] = sum(data["SmallDB"][index])/len(data["SmallDB"][index])
                data["MediumDB"][index] = sum(data["MediumDB"][index])/len(data["MediumDB"][index])
                data["LargeDB"][index] = sum(data["LargeDB"][index])/len(data["LargeDB"][index])
    except:
        print("File name not found or incorrect format.")
        exit()
    return data


def plot(question_num):
    dataBases = (
        "SMallDB",
        "MediumDB",
        "LargeDB",
    )
    data = parser(question_num)
    weight_counts = {
        "Uninformed": np.array([data["SmallDB"][2], data["MediumDB"][2], data["LargeDB"][2]]),
        "Self-Optimized": np.array([data["SmallDB"][1], data["MediumDB"][1], data["LargeDB"][1]]),
                "User-Optimized": np.array([data["SmallDB"][0], data["MediumDB"][0], data["LargeDB"][0]]),

    }
    # Width of each bar
    width = 0.5
    # Define custom colors for each label in the legend
    colors = {
        "User-Optimized": "#FFC000" ,
        "Self-Optimized": "#FF0000",
        "Uninformed": "#0070C0", 
    }
    # Create figure and axis objects
    fig, ax = plt.subplots()
    # Initialize the bottom of the stacked bar to zero
    bottom = np.zeros(3)
    # Loop through each label in the legend
    for label, weight_count in weight_counts.items():
        # Set the color for this label based on the custom color dictionary
        color = colors.get(label, "gray")
        
        # Plot the bar with the custom color
        ax.bar(dataBases, weight_count, width, label=label, bottom=bottom, color=color)
        
        # Update the bottom of the stacked bar for the next iteration
        bottom += weight_count
    # Set title and legend
    number = question_num.replace("Q", "", 1)
    ax.set_title(f"Query {number} (runtime in ms)")
    ax.legend(loc="upper left")
    # Display the graph
    image_file = f"{question_num}A3chart.png"
    if path.exists(image_file):
        os.remove(image_file)
    plt.savefig(f"{question_num}A3chart.png")
    
    
if __name__ == "__main__":
    main()