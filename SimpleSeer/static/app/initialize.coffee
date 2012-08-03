application = require 'application'

$ ->
  $.getJSON '/settings', (data) ->
    _.templateSettings = {interpolate : /\{\{(.+?)\}\}/g}

    application.settings = data.settings
    application.initialize('SimpleSeer')
    Backbone.history.start()
    window[application.appName] = application
