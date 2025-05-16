
Contributions
--------

This is a small scale project under an Open-Source MIT License. There is plenty of room for contributions and they are welcome.
An effort has been made to start the library off with a good foundation for future additions and maintenance. I would request
that be effort be carried forward with the following Contribution Guidelines:

- Open Pull-Requests against the Dev Branch.
- Format the Code with Black
- Use Strict Typing and Pylint + Pylance to catch linting errors
    - Cleanup all linting errors
    - Cleanup most linter warnings. You can occasionally silence warnings using comments where is makes sense.
- Create Tests for added features. 
    - You can use `pip install ".[test]"` to install packages needed to run pytest.
    - Strict Typing & linter guidelines from above are more relaxed for these contributions.
    - TestContainers is used to spawn an empty postgres database for each module of tests as needed. 
    This spawned container can be accessed through the test fixtures under ./tests/conftest.py
- Update the Docs as needed for added tables / Partial Classes
- Provide DocStrings for user facing methods.
- Provide DocStrings & Comments for private methods *as needed* to help with readability
    - fyi, the better the docstrings & comments, the easier it is to vibecode out unit tests.



Documentation
--------

Currently the PsyscaleDB and PsyscaleAsync Classes are comprised of a complex class hierarchy. Mermaid Diagrams of the class structure can be found
under the /Docs/ folder as well as UML Diagrams to describe the main tables within the database. These should help with getting started and understanding
the organization of the library.

While such a complicated class structure isn't ideal for small function set, it was done to allow for the Database Clients to grow more or less indefinitely.
Functionality additions will likely come as additional Partial-Classes. If this is case, additions/updates the the mermaid diagrams would
help retain maintainability.


Future
--------

While there is no road map currently, ideally at some point in the future the core client will be expanded to store information for:
- Stock Splits
- Dividend Payouts
- Earnings Reports
- Custom Composite Symbols
- User-Data Storage / Search / Retrieval 