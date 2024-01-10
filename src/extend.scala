package com.mchange.feedletter.extend

import com.mchange.feedletter.{MastoAnnouncementCustomizer, SubjectCustomizer,SubscribableName,TemplateParamCustomizer}

val MastoAnnouncementCustomizers = Map[SubscribableName,MastoAnnouncementCustomizer](
)
val SubjectCustomizers = Map[SubscribableName,SubjectCustomizer](
)
val ComposeTemplateParamCustomizers = Map[SubscribableName,TemplateParamCustomizer](
)


