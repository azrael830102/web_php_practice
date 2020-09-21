<?php
  require ("ImportLib.php");
  require ("config.php");

  if(!isset($_SESSION)){
    session_start();
    }
  
  if(!isset($_SESSION[$col_username])){
      header("Location:/web_practice1/views/login.php");
      exit();
  }
?>
<script type="text/javascript">
    function logout() {
        if (confirm("Are you sure you want to logout?")) {
            location.replace("/web_practice1/control/logout_php.php");
        }
    }

</script>
