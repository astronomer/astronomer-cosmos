..
   [important] This document is reused across Astronomer OSS Integrations projects, any changes should also be applied in the other repositories

Security
________

This document contains information on how to report security vulnerabilities in Astronomer Cosmos and
how security issues reported to the Astronomer security team are handled.
If you would like to learn more, refer to `https://www.astronomer.io/security/ <https://www.astronomer.io/security/>`_.

At Astronomer, we recognize the critical nature of security and view it as a transparent and collaborative effort.
If you have any concern about the security of any Astronomer public repository, or believe you have uncovered a vulnerability,
please email `oss_security@astronomer.io <mailto:oss_security@astronomer.io>`_.

.. warning::
    Due to the nature of some security vulnerabilities, do not create a GitHub issue to report a vulnerability.


Use of Email for Vulnerability Disclosure
=========================================

Only use the OSS security email to disclose security vulnerabilities.
Astronomer does not accept bug reports, security implementation questions, or other security-related issues at this email address.
If you are a customer of Astronomer, please reach out to your account team if you have any security-related questions or
issues other than vulnerabilities, and they can assist you. Otherwise, this codebase is provided ‘as-is’ in accordance
with its licensing structure.

Scope
=====

When submitting vulnerabilities, please ensure that it is within the scope of the project. Out-of-scope vulnerability reports will be ignored.

In-scope
........

* Code base with tagged releases
* When integrated as specified in the `official Astronomer Cosmos documentation <https://astronomer.github.io/astronomer-cosmos/>`_.

Out-of-scope
............

* Any other codebase, including other Astronomer products
* Astronomer.io website
* Dependencies used in Astronomer Cosmos
* Astronomer Cosmos when modified or run using an unintended configuration
* Other systems integrated with or CSP systems hosting the deployment
* Cookie transfers between browsers

For other products and repositories owned by Astronomer, please refer to their specific security policy or to
`https://www.astronomer.io/vulnerability-disclosure/ <https://www.astronomer.io/vulnerability-disclosure/>`_ for
vulnerabilities associated with Astronomer products.

Required information and how to disclose
====================================

Please send a single, plain-text (not HTML) email for each vulnerability you report.
At a minimum, the report must include the following:

* Brief description/title of the vulnerability
* Steps to recreate the issue
* Contact information

Upon review, we may request additional information including, but not limited to, images or a proof-of-concept video.

Severity
========

The vulnerability severity rating system used internally by Astronomer is not the same as the one used by the Apache Foundation.
Please do not provide a severity for the vulnerability when disclosing, however, providing a CWE (Common Weakness Enumeration) is recommended.

Response Timeframe
==================

Astronomer aims to acknowledge and validate disclosures within 5 business days. Resolutions will be provided in a timely manner.


Follow-up Communication
=======================

Astronomer will handle the disclosures sent to `oss_security@astronomer.io <mailto:oss_security@astronomer.io>`_ on a best-case effort,
usually replying using the same tool.

If the disclosure involves an Astronomer product or repository covered by the BugCrowd Vulnerability Disclosure Platform,
the terms and timeframes may differ and the communication will happen via Bugcrowd.

Partial Safe Harbor
===================

Astronomer will not threaten or bring any legal action against anyone who makes a good faith effort to comply with this
vulnerability disclosure policy. This includes any claim under the DMCA for circumventing technological measures to
protect the services and applications eligible under this policy.

**As long as you comply with this policy:**

* We consider your security research to be "authorized" under the Computer Fraud and Abuse Act (and/or similar state laws), and
* We waive any restrictions in our application Terms of Use and Usage Policies that would prohibit your participation in this policy, but only for the limited purpose of your security research under this policy.

Notification Requirement
========================

* Safe harbor under this policy is only extended if the discoverer of the vulnerability notifies Astronomer as outlined elsewhere in this policy, prior to notifying any other third-party entities, and does not notify any other third-party entities for 90 days after notifying Astronomer, without Astronomer’s prior written approval.
* After notification of Astronomer and the lapse of the 90 day period, it is requested that any publications, third-party releases, or other disseminations of information related to or derived from the vulnerability discovery be coordinated with Astronomer prior.

Right to rescind safe harbor protections
========================================

Astronomer reserves the right to rescind any and all safe harbor protections originally extended to the vulnerability
discoverer in the event that the discoverer, at any point prior to or after notification to Astronomer,
has knowingly and willfully released, published, or otherwise used information related to the discovered vulnerability in a manner that:

1. Maligns or damages the reputation of Astronomer, its customers, or its employees;
2. Is used to conduct malicious attacks against Astronomer systems, regardless of whether material damages occur; or
3. Exacerbates existing vulnerabilities or threats, thereby increasing the risk to Astronomer or its stakeholders.

Extension of safe harbor to third-party systems and services
============================================================

Astronomer systems and services can interconnect with third-party systems and services.
If you submit a report that affects a third-party service through the `vulnerability disclosure program <https://www.astronomer.io/vulnerability-disclosure/>`_,
Astronomer will limit what we share with the affected third party.
Please understand that, while we can authorize your research on Astronomer’s systems and services,
we cannot authorize your efforts on third-party products or guarantee they won’t pursue legal action against you.
If legal action is initiated by a third party against you because of your participation in this vulnerability
disclosure program, and you have complied with our vulnerability disclosure policy, we will take steps to make it known
that your actions were conducted in compliance with this policy.
This is not, and should not be understood as, any agreement on Astronomer's part to defend, indemnify, or otherwise protect you
from any third-party action based on your actions.

You are expected, as always, to comply with all applicable laws.
