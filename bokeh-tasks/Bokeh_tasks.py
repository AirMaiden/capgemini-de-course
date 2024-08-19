import pandas as pd
from bokeh.plotting import figure, output_file, show, save
from bokeh.layouts import column, row
from bokeh.models import HoverTool, ColumnDataSource, Select, CustomJS
from bokeh.transform import factor_cmap
from bokeh.io import curdoc
from bokeh.models.widgets import CheckboxGroup
from bokeh.models.ranges import Range1d
from bokeh.palettes import Category10

def prepare_data(df):
    df = df.dropna(subset=['Age'])
    df = df.dropna(subset=['Cabin'])
    df['Embarked'] = df['Embarked'].fillna('Unknown')

def create_age_group_column(df):
    age_bins = [0, 17, 34, 59, float('inf')]
    age_labels = ['Child', 'Young Adult', 'Adult', 'Senior']
    df['AgeGroup'] = pd.cut(df['Age'], bins=age_bins, labels=age_labels)

def create_age_group_survival_chart(df):
    create_age_group_column(df)
    age_group_survival = df.groupby('AgeGroup',observed=True)['Survived'].mean() * 100
    age_groups = list(age_group_survival.index)
    survival_rates = list(age_group_survival.values)
    source = ColumnDataSource(data=dict(age_groups=age_groups, survival_rates=survival_rates))
    
    output_file("age_group_survival.html")
    palette = ['#66c2a5', '#fc8d62', '#8da0cb', '#e78ac3']
    p = figure(x_range=age_groups, height=400, title="Survival Rates by Age Group",
               toolbar_location=None, tools="", y_axis_label="Survival Rate (%)", x_axis_label="Age Group")
    
    p.vbar(x='age_groups', top='survival_rates', width=0.9, source=source, legend_field="age_groups",
           line_color='white', fill_color=factor_cmap('age_groups', palette=palette, factors=age_groups))
    
    # Add a hover tool
    hover = HoverTool()
    hover.tooltips = [("Age Group", "@age_groups"), ("Survival Rate", "@survival_rates{0.2f}%")]
    p.add_tools(hover)
    
    customize_chart(p)
    save(p)

def create_class_survival_chart(df):
    class_group_survival = df.groupby('Pclass',observed=True)['Survived'].mean() * 100
    class_groups = ['1st', '2nd', '3rd']
    survival_rates = list(class_group_survival.values)
    source = ColumnDataSource(data=dict(class_groups=class_groups, survival_rates=survival_rates))

    output_file("class_group_survival.html")
    palette = ['#66c2a5', '#fc8d62', '#8da0cb']
    p = figure(x_range=class_groups, height=400, title="Survival Rates by Class",
               toolbar_location=None, tools="", y_axis_label="Survival Rate (%)", x_axis_label="Class")

    p.vbar(x='class_groups', top='survival_rates', width=0.9, source=source, legend_field="class_groups",
           line_color='white', fill_color=factor_cmap('class_groups', palette=palette, factors=class_groups))

    # Add a hover tool
    hover = HoverTool()
    hover.tooltips = [("Class", "@class_groups"), ("Survival Rate", "@survival_rates{0.2f}%")]
    p.add_tools(hover)

    customize_chart(p)
    save(p)

def calculate_survival_percentages(df):
    survival_stats = df.groupby(['Pclass', 'Sex'])['Survived'].mean() * 100
    survival_df = survival_stats.reset_index()
    survival_df.columns = ['Class', 'Gender', 'SurvivalRate']
    survival_df['Class_Gender'] = survival_df['Class'].astype(str) + ' ' + survival_df['Gender']
    return survival_df

def create_class_gender_survival_chart(survival_df):
    classes = sorted(survival_df['Class'].unique())
    genders = sorted(survival_df['Gender'].unique())
    source = ColumnDataSource(survival_df)
    output_file("class_gender_survival.html")
    p = figure(x_range=[f'{cls} {gen}' for cls in classes for gen in genders], height=400,
               title="Survival Rates by Class and Gender", toolbar_location=None, tools="",
               x_axis_label="Class and Gender", y_axis_label="Survival Rate (%)")
    
    palette = ['#66c2a5', '#fc8d62']
    colors = factor_cmap('Gender', palette=palette, factors=genders)
    
    p.vbar(x='Class_Gender', top='SurvivalRate', width=0.9, source=source,
           line_color='white', fill_color=colors, legend_field='Gender')
    
    # Add a hover tool
    hover = HoverTool()
    hover.tooltips = [("Class", "@Class"), ("Gender", "@Gender"), ("Survival Rate", "@SurvivalRate{0.2f}%")]
    p.add_tools(hover)

    customize_chart(p)
    save(p)

def create_fare_vs_survival_scatter(df):
    df['Survived'] = df['Survived'].astype(str)  
    df['Pclass'] = df['Pclass'].astype(str)
    source = ColumnDataSource(df)
    output_file('fare_vs_survival.html')

    class_labels = sorted(df['Pclass'].unique())
    colors = Category10[len(class_labels)]  
    color_map = factor_cmap('Pclass', palette=colors, factors=class_labels)
    
    p = figure(height=500, width=800, title="Fare vs. Survival Status by Class",
               x_axis_label="Fare", y_axis_label="Survival Status (0 = No, 1 = Yes)",
               tools="pan,wheel_zoom,box_zoom,reset",
               tooltips=[("Fare", "@Fare"), ("Survival", "@Survived"), ("Class", "@Pclass")])
    
    p.scatter(x='Fare', y='Survived', color=color_map, source=source, legend_field='Pclass',
              size=10, alpha=0.7, marker='circle')
    
    p.y_range = Range1d(start=-0.1, end=1.1)
    p.xgrid.grid_line_color = 'gray'
    p.ygrid.grid_line_color = 'gray'
    p.xaxis.axis_label_standoff = 12
    p.yaxis.axis_label_standoff = 12
    p.axis.axis_label_text_font_size = '12pt'
    p.axis.major_label_text_font_size = '10pt'
    p.legend.orientation = "horizontal"
    p.legend.location = "top_left"
    p.legend.label_text_font_size = '10pt'
    p.legend.border_line_color = None
    p.legend.background_fill_color = 'white'
    p.title.text_font_size = '14pt'
    p.title.text_align = 'center'

    save(p)

def customize_chart(plot):
    p.y_range.start = 0
    p.y_range.end = 100
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_alpha = 0.5
    p.axis.minor_tick_line_color = None
    p.outline_line_color = None
    p.legend.orientation = "horizontal"
    p.legend.location = "top_center"


# Execution
df = pd.read_csv('Titanic-Dataset.csv')
prepare_data(df)
create_age_group_survival_chart(df)
create_class_survival_chart(df)

survival_df = calculate_survival_percentages(df)
create_class_gender_survival_chart(survival_df)
create_fare_vs_survival_scatter(df)