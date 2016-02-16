class SystemsService

  @headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
  @defaultConfig = {headers: @headers}

  constructor: (@$log, @$http, @$q) ->
    @$log.debug "constructing SystemsService"

  systemWithId: (systemId) ->
    @$log.debug "systemWithId #{angular.toJson(systemId, true)}"
    deferred = @$q.defer()

    @$http.get('/_system/' + systemId)
    .success((data, status, headers) =>
      deferred.resolve(data)
    )
    .error((data, status, headers) =>
      deferred.reject(data)
    )
    deferred.promise

  systemsWithWorkspaceId: (workspaceId) ->
    @$log.debug "systemWithWorkspaceId #{angular.toJson(workspaceId, true)}"
    deferred = @$q.defer()

    @$http.get('/_workspace/' + workspaceId + '/systems/')
    .success((data, status, headers) =>
      deferred.resolve()
    )