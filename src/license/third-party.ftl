<#-- THIRD-PARTY.txt (design §14.2). Each license reference is the merged license name, which the gmavenplus step
     resolves to a text in META-INF/licenses/; the NOTICE placeholder is replaced with the component's own NOTICE, and for components not solely
     under Apache-2.0 the license placeholder with the license file the component ships (it carries the copyright
     line that MIT and BSD require; the canonical texts only have placeholders). -->
<#-- License election for components offered under a choice of licenses (design §14.2): the first in this list
     that the component offers. Permissive licenses first, in the order Smile Digital Health set (MIT, Apache, BSD,
     ISC, Creative Commons); then weak copyleft licenses that aren't GPL; the GPL family last. A license outside the
     list is marked for review instead of being chosen. -->
<#assign licensePriority = [
  "MIT", "MIT-0", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "ISC",
  "CC0-1.0", "CC-BY-4.0", "CC-BY-3.0", "CC-BY-SA-4.0",
  "EPL-2.0", "EPL-1.0", "MPL-2.0", "CDDL-1.1",
  "LGPL-2.1", "LGPL-3.0", "GPL-2.0-with-classpath-exception", "GPL-2.0", "GPL-3.0", "AGPL-3.0"]/>
<#function elect licenses>
  <#list licensePriority as p>
    <#if licenses?seq_contains(p)><#return p></#if>
  </#list>
  <#return ""/>
</#function>
<#if dependencyMap?size == 0>
This component has no third-party dependencies.
<#else>
Third-party components and attributions

Where a component is offered under a choice of licenses, all of them are listed, and Smile Digital Health Inc.
distributes it under the one named on its "Elected license" line. Permissive licenses (MIT, Apache, BSD, ISC,
Creative Commons) are elected over any GNU General Public License or similar license.

<#list dependencyMap as e>
<#assign project = e.getKey()/>
<#assign licenses = e.getValue()/>
========================================================================
Component: ${project.name!'Unknown'}
Coordinates: ${project.groupId}:${project.artifactId}:${project.version}
Project: ${project.url!'(no URL in its POM)'}
Licenses: <#list licenses as license>${license}<#if license_has_next>, </#if></#list>
<#if licenses?size gt 1>
<#assign elected = elect(licenses)/>
Elected license: <#if elected?has_content>${elected}<#else>NONE (no license in the priority list; review needed)</#if>
</#if>
License Reference: <#list licenses as license>META-INF/licenses/${license?lower_case?replace(' ', '_')}.txt<#if license_has_next>, </#if></#list>

--- NOTICE ---
[NOTICE_PLACEHOLDER_${project.groupId}:${project.artifactId}:${project.version}]
[COMPONENT_LICENSE_PLACEHOLDER_${project.groupId}:${project.artifactId}:${project.version}]
========================================================================

</#list>
</#if>
