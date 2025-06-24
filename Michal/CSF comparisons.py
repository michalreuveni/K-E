import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import linregress
import numpy as np


def plot_regression_with_r2(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path)

    # Drop rows with missing values in the relevant columns
    df = df[['DQ score', 'new csf score']].dropna()

    # Perform linear regression
    slope, intercept, r_value, p_value, std_err = linregress(df['DQ score'], df['new csf score'])
    r_squared = r_value ** 2

    # Set extended x-range: from 0 to a bit beyond the max DQ score
    x_min = 0
    x_max = max(df['DQ score'].max() * 1.1, 1)  # extend by 10% or default to 1 if all values are small
    x_vals = np.linspace(x_min, x_max, 100)
    y_vals = slope * x_vals + intercept

    # Plot
    sns.set(style="whitegrid")
    plt.figure(figsize=(8, 6))

    # Scatterplot of actual data
    sns.scatterplot(x='DQ score', y='new csf score', data=df, s=50)

    # Plot extended regression line
    plt.plot(x_vals, y_vals, color='red', label='Regression Line')

    # Set axes to start at 0
    plt.xlim(left=0)
    plt.ylim(bottom=0)

    # Labeling
    plt.xlabel('DQ Score')
    plt.ylabel('New CSF Score')
    plt.title('Regression Plot: DQ Score vs New CSF Score')

    # Annotate R²
    plt.text(
        0.05, 0.95,
        f'R² = {r_squared:.3f}',
        transform=plt.gca().transAxes,
        fontsize=12,
        verticalalignment='top',
        bbox=dict(boxstyle="round", facecolor="white", edgecolor="gray")
    )

    plt.legend()
    plt.tight_layout()
    plt.show()


# Example usage
file_path = '/Users/michal.reuveni/Downloads/CSF Summary for python.xlsx'
plot_regression_with_r2(file_path)
