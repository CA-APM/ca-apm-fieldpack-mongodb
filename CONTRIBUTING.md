# Contributing to CA Release Automation

We are glad you want to contribute to CA APM!

## Contribution Process

We have a simple process that utilizes the [GitHub](https://guides.github.com/introduction/flow/index.html) and **Github Issues**:

1. Sign or be added to an existing [Contributor License Agreement (CLA)](https://communities.ca.com/become-a-contributor).
1. Sign up or login to your [GitHub account](https://github.com/signup/free)
1. Fork the repository on GitHub
1. Reporting an issue or making a feature request [here](#issues).
1. Adding features or fixing bugs
1. Create a [Github Pull Request](http://help.github.com/send-pull-requests/)
1. Do [Code Review](#cr) with the **CA APM Engineering Team** or **CA APM Core Committers** on the pull request.

### <a name="pulls"></a> CA APM Pull Requests

CA APM is enterprise grade software. We strive to ensure high quality throughout the CA APM experience. In order to ensure this, we require a couple of things for all pull requests to CA APM:

**Tests:** To ensure high quality code and protect against future regressions, we require all the
  code in CA APM to have at least unit test coverage. See the [spec/unit](https://communities.ca.com/testing)

In addition to this it would be nice to include the description of the problem you are solving
  with your change. You can use [CA APM Issue Template](#issuetemplate) in the description section
  of the pull request.

### <a name="cr"></a> CA APM Code Review Process

The CA APM Code Review process happens on Github pull requests. See
  [this article](https://help.github.com/articles/using-pull-requests) if you're not
  familiar with Github Pull Requests.

Once you a pull request, the **CA APM Engineering Team** or **CA APM Core Committers** will review your code and respond to you with any feedback they might have. The process at this point is as follows:

1. 2 thumbs-ups are required from the **CA APM Engineering Team** or **CA APM Core Committers** for all merges.
1. When ready, your pull request will be tagged with label `Ready For Merge`.
1. Your patch will be merged into `master` including necessary documentation updates
  and you will be included in `CHANGELOG.md`. Our goal is to have patches merged in 4 weeks
  after they are marked to be merged.

### <a name="oh"></a> Developer Office Hours

We hold regular "office hours" on CA Communities that you can join to review contributions together,
ask questions about contributing, or just hang out with CA APM Software employees.  The regularly scheduled CA APM hangouts occur on Mondays and Wednesdays at 3pm Eastern / Noon Pacific.

### Contributor License Agreement (CLA)
Licensing is very important to open source projects. It helps ensure the
  software continues to be available under the terms that the author desired.

CA APM uses [the Eclipse 1.0 license](https://www.github.com/ca-APM/LICENSE)
  to strike a balance between open contribution and allowing you to use the
  software however you would like to.

The license tells you what rights you have that are provided by the copyright holder.
  It is important that the contributor fully understands what rights they are
  licensing and agrees to them. Sometimes the copyright holder isn't the contributor,
  most often when the contributor is doing work for a company.

To make a good faith effort to ensure these criteria are met, CA Release Automation requires an Contributor License Agreement (CLA)
  for contributions. This agreement helps ensure you are aware of the
  terms of the license you are contributing your copyrighted works under, which helps to
  prevent the inclusion of works in the projects that the contributor does not hold the rights
  to share.

It only takes a few minutes to complete a CLA.

You can complete our:
  [CLA](https://www.clahub.com/agreements/CA-APM/ca-ra-nginx-pack) online.
  
### CA APM Obvious Fix Policy

Small contributions such as fixing spelling errors, where the content is small enough
  to not be considered intellectual property, can be submitted by a contributor as a patch,
  without a CLA.

As a rule of thumb, changes are obvious fixes if they do not introduce any new functionality
  or creative thinking. As long as the change does not affect functionality, some likely
  examples include the following:

* Spelling / grammar fixes
* Typo correction, white space and formatting changes
* Comment clean up
* Bug fixes that change default return values or error codes stored in constants
* Adding logging messages or debugging output
* Changes to ‘metadata’ files like Gemfile, .gitignore, build scripts, etc.
* Moving source files from one directory or package to another

**Whenever you invoke the “obvious fix” rule, please say so in your commit message:**

```
------------------------------------------------------------------------
commit 360acb3f82d55d762b0cf9c1d1e99b144a8ed3b5
Author: klinebch <klinebch@gmail.com>
Date:   Fri Oct 10 11:30:11 2014 -0500

  Fix typo in help text.

  Obvious fix.

------------------------------------------------------------------------
```

## <a name="issues"></a> CA APM Issue Tracking

CA APM Issue Tracking is handled using Github Issues.

If you are familiar with CA APM and know the component that is causing you a problem or if you
  have a feature request on a specific component you can file an issue in the corresponding
  Github project. All of our Open Source Software can be found in our
  [Github organization](https://github.com/ca-apm/).

Otherwise you can file your issue in the [CA APM project](https://github.com/ca-apm/<repo>/issues)
  and we will make sure it gets filed against the appropriate project.

In order to decrease the back and forth an issues and help us get to the bottom of them quickly
  we use below issue template. You can copy paste this code into the issue you are opening and
  edit it accordingly.

<a name="issuetemplate"></a>
```
### Version:
[Version of the project installed]

### Environment: [Details about the environment such as the Operating System, cookbook details, etc...]

### Scenario:
[What you are trying to achieve and you can't?]

### Steps to Reproduce:
[If you are filing an issue what are the things we need to do in order to repro your problem?]

### Expected Result:
[What are you expecting to happen as the consequence of above reproduction steps?]

### Actual Result:
[What actually happens after the reproduction steps?]
```

## CA APM Community

CA APM is made possible by a strong community of developers and system administrators. If you have
  any questions or if you would like to get involved in the CA APM community you can check out:

* [CA APM Community](https://communities.ca.com/community/ca-apm) 
