<script lang="ts">
    import { books, fetchFiles } from './bucketFetcher'

    fetchFiles();
  
</script>
  
<div>
    {#each $books as book}
        <p>{book}</p>
    {/each}
</div>
  