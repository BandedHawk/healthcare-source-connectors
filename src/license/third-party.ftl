<#-- THIRD-PARTY.txt (design §14.2). Each license reference is the merged license name, which the gmavenplus step
     resolves to a text in META-INF/licenses/; the NOTICE placeholder is replaced with the component's own NOTICE, and for components not solely
     under Apache-2.0 the license placeholder with the license file the component ships (it carries the copyright
     line that MIT and BSD require; the canonical texts only have placeholders). -->
<#if dependencyMap?size == 0>
This component has no third-party dependencies.
<#else>
Third-party components and attributions

Where a component lists more than one license, it is offered under a choice of them.

<#list dependencyMap as e>
<#assign project = e.getKey()/>
<#assign licenses = e.getValue()/>
========================================================================
Component: ${project.name!'Unknown'}
Coordinates: ${project.groupId}:${project.artifactId}:${project.version}
Project: ${project.url!'(no URL in its POM)'}
Licenses: <#list licenses as license>${license}<#if license_has_next>, </#if></#list>
License Reference: <#list licenses as license>META-INF/licenses/${license?lower_case?replace(' ', '_')}.txt<#if license_has_next>, </#if></#list>

--- NOTICE ---
[NOTICE_PLACEHOLDER_${project.groupId}:${project.artifactId}:${project.version}]
[COMPONENT_LICENSE_PLACEHOLDER_${project.groupId}:${project.artifactId}:${project.version}]
========================================================================

</#list>
</#if>
