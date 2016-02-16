dependencies = [
  'ngRoute',
  'ui.bootstrap',
  'nlptabWebapp.filters',
  'nlptabWebapp.services',
  'nlptabWebapp.controllers',
  'nlptabWebapp.directives',
  'nlptabWebapp.common',
  'nlptabWebapp.routeConfig'
]

app = angular.module('nlptabWebapp', dependencies)

angular.module('nlptabWebapp.routeConfig', ['ngRoute'])
  .config(['$routeProvider', ($routeProvider) ->
    $routeProvider.when('/:workspace', {
      templateUrl: '/assets/partials/main.html'
    }).otherwise({
      redirectTo: '/'
    })
  ])
  .config(['$locationProvider', ($locationProvider) ->
    $locationProvider.html5Mode({
      enabled: true,
      requireBase: false
    })
  ])

@commonModule = angular.module('nlptabWebapp.common', [])
@controllersModule = angular.module('nlptabWebapp.controllers', [])
@servicesModule = angular.module('nlptabWebapp.services', [])
@modelsModule = angular.module('nlptabWebapp.models', [])
@directivesModule = angular.module('nlptabWebapp.directives', [])
@filtersModule = angular.module('nlptabWebapp.filters', [])
