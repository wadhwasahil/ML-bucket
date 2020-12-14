import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
from dash.exceptions import PreventUpdate

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# data preprocessing

df = pd.read_csv("uiuc-gpa-dataset.csv", engine="python").dropna()
grades = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "F", "W"]

filtered_df = df.groupby(["Subject", "Primary Instructor", "Course Title"])[grades].sum()
unique_subjects = df["Subject"].unique()
subject_instructor = df[['Subject','Primary Instructor']].drop_duplicates()
subject_instructor_mapping = subject_instructor.groupby("Subject")["Primary Instructor"].apply(lambda x: x.values.tolist()).to_dict()

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div([
	
	html.H1(
        children=['Grade Distribution (Part 3)'],
        style={
            'textAlign': 'center'
        }
    ),
   html.H3(
        children=['Sahil Wadhwa (sahilw2)'],
        style={
            'textAlign': 'center'
        }
    ),
    html.Div([
	    html.Div(
	    	[html.H6("Subject"),
	        dcc.Dropdown(
	            id='subject-dropdown',
	            options=[{'label': i, 'value': i} for i in unique_subjects],
	            value=unique_subjects[0]
	        )],
	        style = {'width':'40%', 'display': 'inline-block', "margin-left": "20px"},
	    ), 
	    html.Div(
	    	[html.H6("Instructor"),
	    	dcc.Dropdown(id="instructor-dropdown")],
	    	style={'width': '40%', 'display': 'inline-block', "margin-left": "60px"}
	    	),
	    html.Div(id='is-output-container')
	    ], style={'columnCount': 2}),
	    dcc.Graph(
	        id='distribution-graph'
	    )
    ])


# @app.callback(
#     dash.dependencies.Output('dd-output-container', 'children'),
#     [dash.dependencies.Input('subject-dropdown', 'value')])
# def update_output(value):
#     return 'You have selected "{}"'.format(value)


@app.callback(
	dash.dependencies.Output('instructor-dropdown', 'options'),
	dash.dependencies.Input('subject-dropdown', 'value'),
	)
def set_instructor_options(selected_subject):
	return [{'label': i, 'value': i} for i in subject_instructor_mapping[selected_subject]]


@app.callback(
	dash.dependencies.Output('instructor-dropdown', 'value'),
	dash.dependencies.Input('instructor-dropdown', 'options'),
	)
def set_instructor_value(available_options):
	return available_options[0]['value']

@app.callback(
	dash.dependencies.Output('distribution-graph', 'figure'),
	dash.dependencies.Input('subject-dropdown', 'value'),
	dash.dependencies.Input('instructor-dropdown', 'value')
	)
def update_graph(subject, instructor):
	try:
		temp_df = filtered_df.loc[(subject, instructor)].reset_index().melt(id_vars="Course Title", 
	              var_name="Grade", value_name="Count")
		fig = px.histogram(temp_df, x="Grade", y="Count", color="Course Title")
		fig.layout.yaxis.title.text = 'Count'
		return fig
	except:
		raise PreventUpdate
if __name__ == '__main__':
    app.run_server(debug=True)