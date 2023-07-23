# How to run

**Prerequisites**
- Have ``cargo``, the Rust package manager and build tool installed. See [the Rust website](https://www.rust-lang.org/tools/install) for instructions. Note, you may also need to install Visual Studio C++ Build Tools on Windows. Instructions for this are also found on the Rust installation page.
- To run the test front-end (to demonstrate the library works), you will need ``Angular`` installed.
- ``wasm-pack`` is used to help generate the JavaScript interface for the WASM library. Install is using ``cargo install wasm-pack`` (to install the ``wasm-pack`` tool), or by following the [website's instructions](https://rustwasm.github.io/wasm-pack/).

**Building and Running WASM**
- In the ``rust`` subdirectory, run ``wasm-pack build``. If successful, ``rust/target`` and ``rust/pkg`` are created, with ``rust/pkg`` the JavaScript module which can be imported into JavaScript projects.
- To run the front-end to verify the WASM module is working, go into the ``frontend`` directory and use ``ng serve``.
- Open the frontend in your browser and open the Console in DevTools. Clicking the button on the page will call the WASM module, and the results will be visible in the Console.