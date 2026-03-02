IFRAME_SCRIPT = """
<script>
  // Prevent parent hash changes from sending a message back to the parent.
  // This is necessary for making sure the browser back button works properly.
  let hashChangeLock = true;

  window.addEventListener('hashchange', function () {
    if (!hashChangeLock) {
      window.parent.postMessage(window.location.hash);
    }
    hashChangeLock = false;
  });

  window.addEventListener('message', function (event) {
    let msgData = event.data;
    if (typeof msgData === 'string' && msgData.startsWith('#!')) {
      let updateUrl = new URL(window.location);
      updateUrl.hash = msgData;
      hashChangeLock = true;
      history.replaceState(null, null, updateUrl);
    }
  });
</script>
"""
