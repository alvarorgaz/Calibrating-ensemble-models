# Calibrating-ensemble-models in Python

**Author:**

I am Álvaro Orgaz Expósito, a data science student at KTH (Stockholm, Sweden) and a statistician from UPC-UB (Barcelona, Spain).

**Abstract:** 

This repository contains some methods for calibrating Machine Learning ensemble models in Python. The aim of this repository is to overcome the problem of not calibrated predictions. What is that?

Imagine that a company wants to calculate the VaR (Value at Risk) of its customers, it would be the probability of a customer being a churner multiplied by the expected revenue or profit generated by that customer if he does not churn.

Then, the company uses Machine Learning. But some Machine Learning model does not predict the probability of being churner, and just give a score that has no statistical interpretation (for example a prediction range between 0.2 and 0.6). Even if the model is very good at ranking customers from the most probable to the least probable churner (showing for example a fancy AUC), it is insufficient when it comes to providing real businesses with usable insights because of over- or underestimating the churn probability can have a significant financial impact.

Thus, calibrating the model for getting a prediction range between 0 and 1 would be crucial.

**Code:** 

The project has been developed in Python and Jupyter notebook.
