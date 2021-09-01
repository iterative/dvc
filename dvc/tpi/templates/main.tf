terraform {
  required_providers {
    iterative = {
      source = "iterative/iterative"
    }
  }
}

provider "iterative" {}

resource "iterative_machine" "executor" {
  {% if name %}name = "{{name}}"{% endif %}
  {% if cloud %}cloud = "{{cloud}}"{% endif %}
  {% if region %}region = "{{region}}"{% endif %}
  {% if image %}image = "{{image}}"{% endif %}
  {% if instance_type %}instance_type = "{{instance_type}}"{% endif %}
  {% if instance_gpu %}instance_gpu = "{{instance_gpu}}"{% endif %}
  {% if instance_hdd_size %}instance_hdd_size = {{instance_hdd_size}}{% endif %}
  {% if ssh_private %}ssh_private = "{{ssh_private}}"{% endif %}
  {% if spot %}spot = {{spot}}{% endif %}
  {% if spot_price %}spot_price = {{spot_price}}{% endif %}
  {% if startup_script %}startup_script = <<EOF
{{ startup_script }}
  EOF
  {% endif %}
}
