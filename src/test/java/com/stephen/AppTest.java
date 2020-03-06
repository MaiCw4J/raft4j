package com.stephen;

import static org.junit.Assert.assertTrue;

import eraftpb.Eraftpb;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {

        var om = Eraftpb.Message.newBuilder().setFrom(1).setTo(2).build();


        System.out.println(om);

        om = om.toBuilder().setTo(100).build();

        System.out.println(om);

        assertTrue( true );
    }
}
