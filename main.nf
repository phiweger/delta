nextflow.enable.dsl = 2


/*
conda activate nextflow
nextflow run main.nf --genomes input.csv --delta delta.json
*/


process index {
    /*
    The size of the graph can be increased keeping RAM constant by using the
    --disk-swap argument.

    TODO:

    - https://metagraph.ethz.ch/static/docs/quick_start.html#build-graph-in-canonical-mode
    - https://metagraph.ethz.ch/static/docs/quick_start.html#graph-cleaning
    */
    // container 'nanozoo/metagraph_dna:0.1--184de8f'
    // container 'nanozoo/metagraph_proteins:0.1--81a0f32'
    // container 'ratschlab/metagraph:latest'
    // digest 69c90748583b
    // https://hub.docker.com/r/ratschlab/metagraph/tags?page=1&ordering=last_updated
    // https://github.com/ratschlab/metagraph/issues/402
    container 'ghcr.io/ratschlab/metagraph:master'
    publishDir params.results, mode: 'copy', overwrite: true
    cpus params.maxcpu
    memory "${params.maxmem} GB"

    input:
        path(reads)

    output:
        path('graph.dbg')

    """
    mkdir out
    metagraph build -v \
        --parallel ${task.cpus} \
        --mem-cap-gb ${params.maxmem} \
        -k ${params.k} \
        -o graph \
        ${reads} \
    2>&1 | tee log.txt
    """
    // --mode canonical \
}


process annotate {
    /*
    docker run --rm -v $PWD:/data --cpus 8 ratschlab/metagraph:latest stats --print-col-names -a /data/results/graph.column.annodbg
    # Number of columns: 1014

    wc -l input.csv
    # 1014
    */
    // TODO: pass an explicit label, maybe file checksum or UUID?
    // container 'nanozoo/metagraph_dna:0.1--184de8f'
    // https://github.com/ratschlab/metagraph/issues/402
    container 'ghcr.io/ratschlab/metagraph:master'
    publishDir "${params.results}", mode: 'copy', overwrite: true
    cpus params.maxcpu
    memory "${params.maxmem} GB"

    input:
        path(graph)
        path(reads)

    output:
        path('graph.column.annodbg')

    """
    metagraph annotate \
        --parallel ${task.cpus} \
        -i ${graph} \
        --anno-filename \
        -o graph \
        ${reads}
    """
}


process delta {
    container 'ghcr.io/ratschlab/metagraph:master'
    publishDir "${params.results}", mode: 'copy', overwrite: true
    cpus params.maxcpu
    memory "${params.maxmem} GB"

    input:
        path(graph)
        path(annotation)
        path(rules)

    output:
        path('delta.fasta.gz')

    """
    metagraph assemble -v ${graph} --unitigs -a ${annotation} --diff-assembly-rules ${rules} -o delta 2> log.txt
    """
}


workflow {
    genomes = channel.fromPath(params.genomes)
                     .splitCsv(header: false)
                     .map{ name, seq -> seq }

    rules = channel.fromPath(params.delta)

    index(genomes.collect())
    annotate(index.out, genomes.collect())

    delta(index.out, annotate.out, rules)
    /*
    docker run --rm -v $PWD:/data --cpus 8 ghcr.io/ratschlab/metagraph:master stats -a /data/results/graph.column.annodbg /data/results/graph.dbg

    docker run --rm -v $PWD:/data --cpus 8 ratschlab/metagraph:latest align -i /data/results/graph.dbg -a /data/results/graph.column.annodbg --discovery-fraction 0.8 --labels-delimiter ", " /data/qry.fna

    docker run --rm -v $PWD:/data --cpus 8 ratschlab/metagraph:latest query -i /data/results/graph.dbg -a /data/results/graph.column.annodbg --discovery-fraction 0.8 --labels-delimiter ", " /data/qry.fna
    
    0   foobar  GCA_017306715.1_ASM1730671v1_genomic.fna.gz, GCA_001899505.1_ASM189950v1_genomic.fna.gz
    
    docker run --rm -v $PWD:/data --cpus 8 ghcr.io/ratschlab/metagraph:master assemble -v /data/results/graph.dbg --unitigs -a /data/results/graph.column.annodbg --diff-assembly-rules /data/delta.json -o /data/diff_assembled.fa 2> log.txt
    */
}







