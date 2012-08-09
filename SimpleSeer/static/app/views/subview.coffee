require 'lib/view_helper'
View = require './view'

module.exports = class SubView extends View
  options:
    parent: null
    selector: null

  render: () =>
    if @options.append
      if !@options.parent.$('#'+@options.append).length
        @options.parent.$(@options.selector).append('<div id="'+@options.append+'" />')
      @setElement @options.parent.$ '#'+@options.append
    else
      @setElement @options.parent.$ @options.selector
    super
    @
