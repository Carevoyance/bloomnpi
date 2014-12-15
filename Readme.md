This version contains the following differences from the upstream repository:

* Put all database tables into the **bloom** schema.
* Do not perform normalization of the data.
* Track when an NPI has changed using a hash and a revision ID.
