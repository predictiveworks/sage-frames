# SageFrames

<p align="center">
  <img src="https://github.com/predictiveworks/sage-frames/blob/main/images/sage-frames-2022-04-06.jpg" width="800" alt="SageFrames">
</p>

**SageFrames** leverages the Sage Accounting API and retrieves datasets like *PurchaseInvoice*, *SalesInvoice* and more
in form of data science ready Apache Spark DataFrames.

A few lines of code are sufficient to start DataFrame-based data science:
```
/* Dataset name */
val dataset = "PurchaseInvoice"

/* Query parameters to filter the dataset */
val query = Map.empty[String,Any]

val dataframe = new SageReader().read(dataset, query)
```