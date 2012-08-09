/**
 * A collection of useful and reusable
 * date and time tools.
 */

SimpleSeerDateHelper = {
    dayInitials: ['S', 'M', 'T', 'W', 'T', 'F', 'S'],
    monthNames: ['January','February','March','April','May','June','July','August','September','October','November','December'],
    monthDays: [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    
    offsetMonth: function(date, span) {
        var theDate = new Date(date.toISOString());
        theDate.setMonth(theDate.getMonth() + span);
        return theDate;
    },
    
    prettyDate: function(date) {
        return date.toLocaleDateString().match(/\w+\s\d+\,\s\d+/)[0];
    },
    
    prettyTime: function(date) {
        return date.toTimeString().split(" ")[0]
    },
    
    flushtime: function(timeString) {
        timeString = timeString.split(":");
        if( timeString.length == 2 ) { timeString.push("00"); }
        else if( timeString.length == 1 ) { timeString = ["12", "00", "00"]; }
        return timeString.join(":");
    }
}

/**
 * The DateTimerRange widget definition.
 */

$.widget("ui.datetimerange", {
    options: {
        startDate: (new Date()),
        endDate: (new Date())
    },
    
    _theMonth: "",
    _inChange: false,
    window: "",
    leftSide: "",
    rightSide: "",
    calendarModels: [],
    calendarViews: [],
    
    _create: function() {
        var self = this;
        var options = this.options;
        
        // Set the beginning month.
        if( options.endDate - options.startDate < 2592000000 ) {
            self._theMonth = new Date(options.endDate);
        } else {
            self._theMonth = SimpleSeerDateHelper.offsetMonth(options.startDate, 1);
        }
        
        var element = this.element;
        element.bind("focus", function(e, ui) { self.appear(e, ui); });
        
        this.window = $("<div></div>")
                        .hide()
                        .addClass("ui-datetimerange")
                        .css({
                            "top": element.offset().top + element.height(),
                            "left": element.offset().left
                        })
                        .appendTo("body");
                        
        this.leftSide = $("<div></div>")
                            .addClass("left")
                            .appendTo(this.window);
        
        this.rightSide = $(
            '<div>'+
                '<div class="block">'+
                    '<label>Date:</label>'+            
                    '<input class="ss-date-from" type="text">'+
                    ' - '+
                    '<input class="ss-date-to" type="text">'+
                '</div>'+
                '<div class="block">'+                
                    '<label>Time:</label>'+            
                    '<input class="ss-time-from" type="text" value="'+SimpleSeerDateHelper.prettyTime(options.startDate)+'">'+
                    ' - '+
                    '<input class="ss-time-to" type="text" value="'+SimpleSeerDateHelper.prettyTime(options.endDate)+'">'+
                '</div>'+
                '<div class="bottom">'+                
                    '<button class="apply">Apply</button>'+            
                '</div>'+                
            '</div>'
        )
        .addClass("right")
        .appendTo(this.window);
        
        var goBackMonth = $("<button>&laquo;</button>").addClass("switch").appendTo(this.leftSide);
        
        for(var e=0; e<3; e++) {
            self.calendarViews[e] = $("<div></div>")
                                        .addClass("ss-calendar")
                                        .appendTo(this.leftSide);
        
            self.calendarModels[e] = new Calendar(self.calendarViews[e], {
                startDate: options.startDate,
                endDate: options.endDate,
                month: SimpleSeerDateHelper.offsetMonth(self._theMonth, -1 + e)
            });
        }
        
        var goNextMonth = $("<button>&raquo;</button>").addClass("switch").appendTo(this.leftSide);
        
        /**
         * Event handling and other miscellaneous
         * tweaks to the interface.
         */

        $(".ss-calendar .date").live("click", function() {
            var eleDate = new Date($(this).attr("data-date"));
            
            if( self._inChange == false ) {
                self._inChange = true;
                options.startDate = eleDate;
                options.endDate = eleDate;
                
                $(".ss-date-from").removeClass("alter");
                $(".ss-date-to").addClass("alter");
            } else if( eleDate >= options.startDate ) {
                self._inChange = false;
                options.endDate = eleDate;
                
                $(".ss-date-from").addClass("alter");
                $(".ss-date-to").removeClass("alter");
            }
            
            self.updateCalendars();
        });
        
        $(".ss-time-from, .ss-time-to").blur(function() {
           $(this).attr("value", SimpleSeerDateHelper.flushtime($(this).attr("value")));
        });
                
        goBackMonth.click(function() {
           self._theMonth = SimpleSeerDateHelper.offsetMonth(self._theMonth, -1);
           for(var e=0; e<self.calendarModels.length; e++) {
                self.calendarModels[e].setMonth(SimpleSeerDateHelper.offsetMonth(self._theMonth, -1 + e));
           }
        });
        
        goNextMonth.click(function() {
           self._theMonth = SimpleSeerDateHelper.offsetMonth(self._theMonth, 1);
           for(var e=0; e<self.calendarModels.length; e++) {
                self.calendarModels[e].setMonth(SimpleSeerDateHelper.offsetMonth(self._theMonth, -1 + e))
           }
        });
        
        var applyButton = self.rightSide.find(".apply");
        applyButton.click(function() {
            _sd = self.options.startDate;
            _ed = self.options.endDate;
            
            _st = self.window.find(".ss-time-from").attr("value").split(":");
            _et = self.window.find(".ss-time-to").attr("value").split(":");
            
            self._trigger("onUpdate", null, {
                startDate: new Date(_sd.getYear() + 1900, _sd.getMonth(), _sd.getDate(), _st[0], _st[1], _st[2]),
                endDate: new Date(_ed.getYear() + 1900, _ed.getMonth(), _ed.getDate(), _et[0], _et[1], _et[2])
            });
            
            self.disappear();
        });
        
        this.updateCalendars();
        this._onUpdate();
    },
    
    destroy: function() {
        $.Widget.prototype.destroy.call(this);
    },
    
    _setOption: function(key, value) {
        $.Widget.prototype_setOption.apply(this, arguments);
    },
    
    _onUpdate: function() {
        var element = this.element;
        var fromDate = SimpleSeerDateHelper.prettyDate(this.options.startDate);
        var toDate = SimpleSeerDateHelper.prettyDate(this.options.endDate);
        
        this.window.find(".ss-date-from").attr("value", fromDate);
        this.window.find(".ss-date-to").attr("value", toDate);     
    },
    
    updateCalendars: function() {
        var self = this;

        for(var e=0; e<self.calendarModels.length; e++) {
             self.calendarModels[e].setStartDate(self.options.startDate);
             self.calendarModels[e].setEndDate(self.options.endDate);
             self.calendarModels[e].setMode(self._inChange);
        }
        
        self._onUpdate();
    },
    
    /**
     * Getters and Setters
     */
    
    setStartDate: function(date) {
        options.startDate = date;
        this.updateCalendars();
    },
    
    setEndDate: function(date) {   
        options.endDate = date;
        this.updateCalendars();
    },
    
    /**
     * Widget specific code
     */
    
    appear: function(e, ui) {
        this.window.show();        
    },
    
    disappear: function(e, ui) {
        this.window.hide();
    }
});

/**
 * Calendar class will take in a container
 * and some settings to create the markup
 * for the calendar.
 */

function Calendar(element, settings) {
    var settings = settings;
    
    var table = $('<table border="0" cellspacing="0" cellpadding="0"></table>')
                    .append($('<caption></caption><thead><tr></tr></thead><tbody></tbody>'))
                    .appendTo(element);
    
    for(var i=0; i<SimpleSeerDateHelper.dayInitials.length; i++) {
        table.find("thead tr").append(
            '<th class="cell">'+ SimpleSeerDateHelper.dayInitials[i] + '</td>'
        );
    }
    
    function markup() {
        var month = settings.month.getMonth();
        var year = settings.month.getFullYear();
        var day = settings.month.getDate(); 
        var firstDayDate = new Date(year, month, 1);
        var firstDay = firstDayDate.getDay();
        
        table.find("caption").html(SimpleSeerDateHelper.monthNames[month] + " " + year);
        table.find("tbody").html("");
        
        var j = 0;
        for(var w=0; w<6; w++) {
            var row = $("<tr></tr>").appendTo(table.find("tbody"));
            
            for(var d=0; d<7; d++) {
                var cell = $("<td></td>").addClass("cell").appendTo(row);
                var jDate = new Date(year, month, (j-firstDay+1));
                var sDate = new Date(settings.startDate.getYear() + 1900, settings.startDate.getMonth(), settings.startDate.getDate())
                var eDate = new Date(settings.endDate.getYear() + 1900, settings.endDate.getMonth(), settings.endDate.getDate())
                
                if ( (j < firstDay) || (j > (getDaysInMonth(month, year) + firstDay - 1)) ) {
                
                } else {
                    cell.html(j - firstDay + 1).addClass("date").attr("data-date", jDate.toISOString());
                    
                    if( settings.isEdit && jDate < settings.startDate ) { cell.addClass("static"); }
                    if( jDate >= sDate && jDate <= eDate ) { cell.addClass("selected"); }
                }
                
                j++;
            }
            
            //if( row.find("td:eq(0)").html() == "" && row.find("td:eq(6)").html() == "" ) { row.remove(); }
        }
    }
    
    function getDaysInMonth(month,year){
        if ((month==1)&&(year%4==0)&&((year%100!=0)||(year%400==0))){
          return 29;
        } else {
          return SimpleSeerDateHelper.monthDays[month];
        }
    }
    
    function draw() {
        markup();
    }
    
    this.setStartDate = function(date) {
        settings.startDate = date;
        draw();
    }
    
    this.setEndDate = function(date) {
        settings.endDate = date;
        draw();
    }
    
    this.setMonth = function(date) {
        settings.month = date;
        draw();
    }
    
    this.setMode = function(isEdit) {
        settings.isEdit = isEdit;
        draw();
    }
    
    draw();
    
    this.draw = draw;
}     