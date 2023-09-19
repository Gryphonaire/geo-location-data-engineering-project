## Description
This folder has zip files which were added as Lambda layers for the code execution.
You can create these layers using the steps mentioned [here.](https://towardsdatascience.com/building-custom-layers-on-aws-lambda-35d17bd9abbb)
The order in which the layers are added matters in AWS Lambda !!

Python packages in multiple_layers.zip:
```
Requests
Regex
OpenCage
BeautifulSoup4
GeoPy
```
We had to create a new layer for requests package due to the [issue mentioned here](https://github.com/psf/requests/issues/6443#issuecomment-1535667256)



