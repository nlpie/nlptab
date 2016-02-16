package models

import java.util.Date

case class System(id: Option[Long] = None,
                  name: String,
                  description: String,
                  created: Option[Date] = None,
                  version: String)
