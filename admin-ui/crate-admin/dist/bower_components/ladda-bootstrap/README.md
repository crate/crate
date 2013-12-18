# Ladda for Bootstrap 3

Buttons with built-in loading indicators, effectively bridging the gap between action and feedback. 

[Check out the demo page](http://msurguy.github.io/ladda-bootstrap/).

## Instructions

Please see the demo page for the details: (http://msurguy.github.io/ladda-bootstrap/).

#### HTML

Ladda buttons must be given the class ```ladda-button``` and the button label needs to have the ```ladda-label``` class. The ```ladda-label``` will be automatically created if it does not exist in the DOM. Below is an example of a button which will use the expand-right animation style.

```html
<button class="ladda-button" data-style="expand-right"><span class="ladda-label">Submit</span></button>
```

Buttons accepts three attributes:
- **data-style**: one of the button styles, full list in [demo](http://lab.hakim.se/ladda/) *[required]*
- **data-color**: green/red/blue/purple/mint
- **data-size**: xs/s/l/xl, defaults to medium
- **data-spinner-size**: 40, pixel dimensions of spinner, defaults to dynamic size based on the button height
- **data-spinner-color**: A hex code or any [named CSS color](http://css-tricks.com/snippets/css/named-colors-and-hex-equivalents/). 

#### JavaScript

If you will be using the loading animation for a form that is submitted to the server (always resulting in a page reload) you can use the ```bind()``` method:

```javascript
// Automatically trigger the loading animation on click
Ladda.bind( 'input[type=submit]' );

// Same as the above but automatically stops after two seconds
Ladda.bind( 'input[type=submit]', { timeout: 2000 } );
```

If you want JavaScript control over your buttons you can use the following approach:

```javascript
// Create a new instance of ladda for the specified button
var l = Ladda.create( document.querySelector( '.my-button' ) );

// Start loading
l.start();

// Will display a progress bar for 50% of the button width
l.setProgress( 0.5 );

// Stop loading
l.stop();

// Toggle between loading/not loading states
l.toggle();

// Check the current state
l.isLoading();
```

All loading animations on the page can be stopped by using:

```javascript
Ladda.stopAll();
```

## Browser support

The project is tested in Chrome and Firefox. It Should Work™ in the current stable releases of Chrome, Firefox, Safari as well as IE9 and up.

## License

MIT licensed

Copyright (C) 2013 Hakim El Hattab, http://hakim.se