Imports [Visit Finland](https://datahub.visitfinland.com/) (geo)data for [QGIS](https://qgis.org/) analysis via [PostGIS](https://postgis.net/).

### Requirements

 - PostGIS DB
 - Python 3
 - uv
 - API key to visitfinland datahub

### Usage
 
Look at vscode run json for step 1 (dump all from graphql to json file) and step 2 (store to db).

### TODO

 - Investigate duplicate ids in query, maybe querying wrong!!!
 - Extract more fields to columns (especially tags)
 - Continuous updating (kind of works already)
 - Schema from GraphQL instead
 - DB indexes? *not needed for now*

### License

This work is licensed under the terms of the [MIT](https://opensource.org/licenses/MIT) license.  

Copyright (c) 2026 University of Turku, Lauri Koivunen.

### Disclaimer

Large parts of this codebase were coded with the use of AI. Code is not particularly reusable. Only for dumping and exploring the dataset.

### Acknowledgements

Code artifact produced as part of the [Digiretki](https://tt.utu.fi/sweng/digiretki/) project.

![Co-funded by the European Union](funding.png)
