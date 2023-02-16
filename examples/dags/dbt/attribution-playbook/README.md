# Attribution playbook
This dbt project is a worked example to demonstrate how to model customer
attribution. **Check out the full write-up [here](https://blog.getdbt.com/modeling-marketing-attribution/),
as well as the documentation site for this project [here](https://www.getdbt.com/attribution-playbook/#!/overview).**

Note that this project is not a package -- it is not intended to be installed in
your own dbt project, but instead provides a good starting point for building
similar data models for your own business.

The SQL in this project is compatible with Snowflake¹.

If you want to run this project yourself to play with it (assuming you have
dbt installed):
1. Clone this repo.
2. Create a profile named `playbook`, or update the `profile:` key in the
`dbt_project.yml` file to point to an existing profile ([docs](https://docs.getdbt.com/docs/configure-your-profile)).
3. Run `dbt deps`.
4. Run `dbt seed`.
5. Run `dbt run` -- if you are using a warehouse other than Snowflake, you may
find that you have to update some SQL to be compatible with your warehouse.
6. Run `dbt test`.

-----
1. We decided to _not_ make the SQL multi-warehouse compatible since this project
is intended to be a demonstration, rather than a package. Making this project
multi-warehouse compatible would complicate the SQL.
