from airflow.api.common.experimental.trigger_dag import trigger_dag  
from airflow import configuration as conf  
from airflow.plugins_manager import AirflowPlugin  
from airflow.models import DagBag  
from flask import render_template_string, request, Markup  
from airflow.utils import timezone


trigger_template = """  
<head></head>  
<body>  
    <a href="/home">Home</a>
      {% if messages %}
        <ul class=flashes>
        {% for message in messages %}
          <li>{{ message }}</li>
        {% endfor %}
        </ul>
      {% endif %}
    <h1>Manual Trigger</h1>
    <div class="widget-content">
       <form id="triggerForm" method="post">
          <label for="dag">Select a dag:</label>
          <select name="dag" id="selected_dag">
              <option value=""></option>
              {%- for dag_id, dag_arguments in dag_data.items() %}
              <option value="{{ dag_id }}" {% if dag_id in selected %}selected="selected"{% endif %}>{{ dag_id }}</option>
              {%- endfor %}
          </select>
          <div id="dag_options">
              {%- for dag_id, dag_arguments in dag_data.items() %}
                  <div id="{{ dag_id }}" style='display:none'>
                    {% if dag_arguments %}
                        <b>Arguments to trigger dag {{dag_id}}:</b><br>
                    {% endif %}
                    {% for dag_argument_name, _ in dag_arguments.items() %}
                        <input type="text" id="{{ dag_argument_name }}" name="{{dag_id}}-{{ dag_argument_name }}" placeholder="{{ dag_argument_name }}" ><br>
                    {% endfor %}
                  </div>
              {%- endfor %}
          </div>
          <br>
          <input type="submit" value="Trigger" class="btn btn-secondary">
        {% if csrf_token %}
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        {% endif %}
       </form>
    </div>
</body>  
<script type="text/javascript">  
var selectedDag = document.getElementById("selected_dag");  
var previous;  
selectedDag.addEventListener("change", function() {  
    if (previous) previous.style.display = "none"
    var dagOptions = document.getElementById(selectedDag.value);
    dagOptions.style.display = "block";
    previous = dagOptions;
});
</script>  
"""


def trigger(dag_id, trigger_dag_conf):  
    """Function that triggers the dag with the custom conf"""
    execution_date = timezone.utcnow()

    dagrun_job = {
        "dag_id": dag_id,
        "run_id": f"manual__{execution_date.isoformat()}",
        "execution_date": execution_date,
        "replace_microseconds": False,
        "conf": trigger_dag_conf
    }
    r = trigger_dag(**dagrun_job)
    return r


# if we dont have RBAC enabled, we setup a flask admin View
from flask_admin import BaseView, expose  
class FlaskAdminTriggerView(BaseView):  
    @expose("/", methods=["GET", "POST"])
    def list(self):
        if request.method == "POST":
            print(request.form)
            trigger_dag_id = request.form["dag"]
            trigger_dag_conf = {k.replace(trigger_dag_id, "").lstrip("-"): v for k, v in request.form.items() if k.startswith(trigger_dag_id)}
            dag_run = trigger(trigger_dag_id, trigger_dag_conf)
            messages = [f"Dag {trigger_dag_id} triggered with configuration: {trigger_dag_conf}"]
            dag_run_url = DAG_RUN_URL_TMPL.format(dag_id=dag_run.dag_id, run_id=dag_run.run_id)
            messages.append(Markup(f'<a href="{dag_run_url}" target="_blank">Dag Run url</a>'))
            dag_data = {dag.dag_id: getattr(dag, "trigger_arguments", {}) for dag in DagBag().dags.values()}
            return render_template_string(trigger_template, dag_data=dag_data, messages=messages)
        else:
            dag_data = {dag.dag_id: getattr(dag, "trigger_arguments", {}) for dag in DagBag().dags.values()}
            return render_template_string(trigger_template, dag_data=dag_data)
v = FlaskAdminTriggerView(category="Extra", name="Manual Trigger")



# If we have RBAC, airflow uses flask-appbuilder, if not it uses flask-admin
from flask_appbuilder import BaseView as AppBuilderBaseView, expose  
class AppBuilderTriggerView(AppBuilderBaseView):  
    @expose("/", methods=["GET", "POST"])
    def list(self):
        if request.method == "POST":
            print(request.form)
            trigger_dag_id = request.form["dag"]
            trigger_dag_conf = {k.replace(trigger_dag_id, "").lstrip("-"): v for k, v in request.form.items() if k.startswith(trigger_dag_id)}
            dag_run = trigger(trigger_dag_id, trigger_dag_conf)
            messages = [f"Dag {trigger_dag_id} triggered with configuration: {trigger_dag_conf}"]
            dag_run_url = DAG_RUN_URL_TMPL.format(dag_id=dag_run.dag_id, run_id=dag_run.run_id)
            messages.append(Markup(f'<a href="{dag_run_url}" target="_blank">Dag Run url</a>'))
            dag_data = {dag.dag_id: getattr(dag, "trigger_arguments", {}) for dag in DagBag().dags.values()}
            return render_template_string(trigger_template, dag_data=dag_data, messages=messages)
        else:
            dag_data = {dag.dag_id: getattr(dag, "trigger_arguments", {}) for dag in DagBag().dags.values()}
            return render_template_string(trigger_template, dag_data=dag_data)


v_appbuilder_view = AppBuilderTriggerView()  
v_appbuilder_package = {"name": "Manual Trigger",  
                        "category": "Extra",
                        "view": v_appbuilder_view}



# Defining the plugin class
class TriggerViewPlugin(AirflowPlugin):  
    name = "triggerview_plugin"
    admin_views = [v] # if we dont have RBAC we use this view and can comment the next line
    appbuilder_views = [v_appbuilder_package] # if we use RBAC we use this view and can comment the previous line