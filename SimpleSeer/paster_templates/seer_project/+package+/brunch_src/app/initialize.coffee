application = require 'application'

$ ->
  $.getJSON '/settings', (data) ->
    _.templateSettings = {interpolate : /\{\{(.+?)\}\}/g}

    window.SimpleSeer = application
    #TODO: bind initalize events
    application._init(data.settings)
    application.initialize()
    Backbone.history.start()
    # Freeze the object
    Object.freeze? application
