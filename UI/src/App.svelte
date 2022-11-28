<script>
  import GitHubLogo from './assets/github.svg';
  import FileSelector from './lib/FileSelector.svelte';
</script> 

<main>
  <h1>Cloud Computing - Map Reducer</h1>

  <div class="card">
    <FileSelector></FileSelector>
  </div>

  <p>
    Source code <a href="https://github.com/gustavo-bertoldi/CC-MapReducer"><img src={GitHubLogo} alt="Source code"  target="_blank" rel="noreferrer" class="github-logo"/></a>
  </p>
</main>

<style>
  .github-logo {
    width: 1.5em;
    height: 1.5em;
    vertical-align: middle;
  }

  .github-logo:hover {
    color: white;
  }
</style>
