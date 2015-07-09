# pdi-datavault-plugin

**DV-loaders** provide custom Transformation steps for loading Data Vault compliant objects: Hub, Link and Satellite. These are developed using plugin extension capability offered by Pentaho Data Integration tool (PDI, aka Kettle).

With these custom Steps, you can:

  * Load quickly and easily **Hub**, **Link** and **Satellite**
  * Define as many Steps as needed inside the same Transformation
  * Load mandatory attributes compliant with logical Data Vault rules
  * Load non-mandatory fields as-is (ex. batch-Id for auditing)
  * Adjust buffer size to fine-tune performance


Please refer to <PDI Plugin> (TODO: link to description from blog) for more details.
