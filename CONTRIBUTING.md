# Contribution Guidelines

Thank you for thinking of contributing to FiloDB!   We welcome all contributions through Github Pull Requests.  
When you create a new PR, please be sure to review the guidelines below. Then create a branch based on `develop` (not `main`).

## <a name="pullrequest"></a> Pull Requests Guidelines
Pull requests are the only means by which you can contribute to this project, please follow the following steps when submitting pull requests :

1. Create your pull request.
2. Fill the pull request template with all of the required elements
3. CI tests kick in and report back any issues, you should fix these issues before continuing.
4. The reviewer(s) uses Github review system so you will know if the reviewer requested any changes, approved the Pull Request or simply added comments.
5. If any changes are requested please fix them and then once you are ready ask for a new review

When pull requests are merged, they will be squashed by default.

## Commit Message Format
A commit message is made up of a **header**, a **body** and a **footer**.  The header has a special
format that includes a **type**, a **scope** and a **subject**:

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

* The **header** and **scope** are mandatory
* Lines shouldn't exceed 100 characters
* PLEASE ensure that there are no references to company-internal URLs, Jira or equivalent bug tracking systems, or confidential information.

#### Type
Must be one of the following:

* **feat**: A new feature
* **fix**: A bug fix
* **refactor**: Refactoring change
* **docs**: Documentation only changes
* **style**: Style changes (white-space, formatting, missing semi-colons, etc)
* **test**: Adding missing tests
* **misc**: Changes to the build process or auxiliary tools and libraries

#### Scope
The scope sets the scope or module of the commit, for example `core`, `kafka`, etc...

#### Subject
Brief commit description

* use the imperative, present tense: "change" not "changed" nor "changes"
* don't capitalize first letter
* no dot (.) at the end

#### Body
Extended commit description and just as in the **subject**, use the imperative, present tense: "change" not "changed" nor "changes".
The body should include the motivation for the change and contrast this with previous behaviour if any.

#### Footer

* any information about **Breaking Changes**

**Breaking Changes** should start with the word `BREAKING CHANGES:` followed by a new line and a list of breaking changes.
